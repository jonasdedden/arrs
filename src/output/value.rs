//! Per-arrow-type value formatting for both CSV and JSONL.
//!
//! CSV rules:
//! - Null → empty cell.
//! - Complex types (list, struct, map, binary, duration, interval, union, …) → error.
//! - Binary & binary-view types → error (user should use JSONL).
//! - Timestamps, dates, times → ISO-8601 strings.
//! - Floats: `NaN`, `inf`, `-inf`.
//! - Decimals: standard decimal-point string.
//!
//! JSONL rules:
//! - Null → `null`.
//! - Binary → string of `\xHH` escapes (lowercase hex).
//! - Timestamps → ISO-8601 (with tz suffix if zoned).
//! - Floats: `"NaN"`, `"Infinity"`, `"-Infinity"` as JSON *strings* (numbers otherwise).
//! - Decimals → raw unscaled integer (may exceed JavaScript's safe range).
//! - List / Struct / Map / FixedSizeList → JSON array / object.
//! - Dictionary → decoded to its value type.

use std::sync::Arc;

use arrow_array::cast::{AsArray, as_dictionary_array};
use arrow_array::types::{
    Decimal32Type, Decimal64Type, Decimal128Type, Decimal256Type, DurationMicrosecondType,
    DurationMillisecondType, DurationNanosecondType, DurationSecondType, Float16Type, Float32Type,
    Float64Type, Int8Type, Int16Type, Int32Type, Int64Type, IntervalDayTimeType,
    IntervalMonthDayNanoType, IntervalYearMonthType, Time32MillisecondType, Time32SecondType,
    Time64MicrosecondType, Time64NanosecondType, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType, UInt8Type, UInt16Type,
    UInt32Type, UInt64Type,
};
use arrow_array::{Array, Date32Array, Date64Array};
use arrow_schema::{DataType, Schema, TimeUnit};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime};
use serde_json::{Map as JsonMap, Value};

use crate::Result;
use crate::cli::BinaryFormat;
use crate::error::Error;

// ---------- schema validation ----------

/// Reject types that cannot be represented losslessly in CSV. All four binary
/// flavors are accepted under any `--binary-format`: the value is rendered as
/// hex, base64, or the literal "BINARY_DATA" placeholder.
pub fn validate_csv_schema(schema: &Schema) -> Result<()> {
    for field in schema.fields() {
        validate_csv_type(field.name(), field.data_type())?;
    }
    Ok(())
}

fn validate_csv_type(col: &str, ty: &DataType) -> Result<()> {
    use DataType::*;
    match ty {
        Null
        | Boolean
        | Int8
        | Int16
        | Int32
        | Int64
        | UInt8
        | UInt16
        | UInt32
        | UInt64
        | Float16
        | Float32
        | Float64
        | Utf8
        | LargeUtf8
        | Date32
        | Date64
        | Time32(_)
        | Time64(_)
        | Timestamp(_, _)
        | Decimal32(_, _)
        | Decimal64(_, _)
        | Decimal128(_, _)
        | Decimal256(_, _)
        | Binary
        | LargeBinary
        | BinaryView
        | FixedSizeBinary(_) => Ok(()),

        Dictionary(_, value_ty) => validate_csv_type(col, value_ty),

        Utf8View
        | List(_)
        | LargeList(_)
        | FixedSizeList(_, _)
        | ListView(_)
        | LargeListView(_)
        | Struct(_)
        | Map(_, _)
        | Union(_, _)
        | RunEndEncoded(_, _)
        | Duration(_)
        | Interval(_) => Err(Error::UnsupportedCsvType {
            column: col.to_string(),
            data_type: format!("{ty:?}"),
        }),
    }
}

// ---------- CSV cell formatting ----------

/// Format a single cell for CSV. Returns `None` for null (writer emits an empty field).
pub fn csv_cell(
    array: &dyn Array,
    row: usize,
    binary_format: BinaryFormat,
) -> Result<Option<String>> {
    if array.is_null(row) {
        return Ok(None);
    }
    Ok(Some(csv_non_null(array, row, binary_format)?))
}

fn csv_non_null(array: &dyn Array, row: usize, binary_format: BinaryFormat) -> Result<String> {
    use DataType::*;
    Ok(match array.data_type() {
        Null => String::new(),
        Boolean => array.as_boolean().value(row).to_string(),
        Int8 => array.as_primitive::<Int8Type>().value(row).to_string(),
        Int16 => array.as_primitive::<Int16Type>().value(row).to_string(),
        Int32 => array.as_primitive::<Int32Type>().value(row).to_string(),
        Int64 => array.as_primitive::<Int64Type>().value(row).to_string(),
        UInt8 => array.as_primitive::<UInt8Type>().value(row).to_string(),
        UInt16 => array.as_primitive::<UInt16Type>().value(row).to_string(),
        UInt32 => array.as_primitive::<UInt32Type>().value(row).to_string(),
        UInt64 => array.as_primitive::<UInt64Type>().value(row).to_string(),
        Float16 => format_f32_csv(f32::from(array.as_primitive::<Float16Type>().value(row))),
        Float32 => format_f32_csv(array.as_primitive::<Float32Type>().value(row)),
        Float64 => format_f64_csv(array.as_primitive::<Float64Type>().value(row)),
        Utf8 => array.as_string::<i32>().value(row).to_string(),
        LargeUtf8 => array.as_string::<i64>().value(row).to_string(),
        Date32 => format_date32(
            array
                .as_any()
                .downcast_ref::<Date32Array>()
                .unwrap()
                .value(row),
        ),
        Date64 => format_date64(
            array
                .as_any()
                .downcast_ref::<Date64Array>()
                .unwrap()
                .value(row),
        ),
        Time32(unit) => format_time32(time32_value_at(array, *unit, row), *unit),
        Time64(unit) => {
            let v = match unit {
                TimeUnit::Microsecond => array.as_primitive::<Time64MicrosecondType>().value(row),
                TimeUnit::Nanosecond => array.as_primitive::<Time64NanosecondType>().value(row),
                TimeUnit::Second | TimeUnit::Millisecond => {
                    unreachable!("arrow disallows Time64 with second/millisecond units")
                }
            };
            format_time64(v, *unit)
        }
        Timestamp(unit, tz) => format_timestamp_at(array, *unit, tz.as_deref(), row),
        Decimal32(_, scale) => insert_decimal_point(
            &array.as_primitive::<Decimal32Type>().value(row).to_string(),
            *scale,
        ),
        Decimal64(_, scale) => insert_decimal_point(
            &array.as_primitive::<Decimal64Type>().value(row).to_string(),
            *scale,
        ),
        Decimal128(_, scale) => {
            format_decimal128_csv(array.as_primitive::<Decimal128Type>().value(row), *scale)
        }
        Decimal256(_, scale) => format_decimal256_csv(
            array
                .as_primitive::<Decimal256Type>()
                .value(row)
                .to_string(),
            *scale,
        ),
        Binary => encode_binary(array.as_binary::<i32>().value(row), binary_format),
        LargeBinary => encode_binary(array.as_binary::<i64>().value(row), binary_format),
        BinaryView => encode_binary(array.as_binary_view().value(row), binary_format),
        FixedSizeBinary(_) => encode_binary(array.as_fixed_size_binary().value(row), binary_format),
        Dictionary(key_ty, _) => {
            let values = dict_values(array, key_ty);
            let logical_index = dict_logical_index(array, key_ty, row);
            csv_non_null(values.as_ref(), logical_index, binary_format)?
        }
        // Anything else should have been rejected by validate_csv_schema.
        other => {
            return Err(Error::UnsupportedCsvType {
                column: String::new(),
                data_type: format!("{other:?}"),
            });
        }
    })
}

// ---------- Table cell formatting ----------

/// Format a single cell for table output. Strictly more permissive than CSV:
/// primitives render exactly the same way, but nested types (lists, structs,
/// maps, …) fall through to the JSONL renderer and are serialised compactly.
/// Null becomes an empty string.
pub fn table_cell(array: &dyn Array, row: usize, binary_format: BinaryFormat) -> Result<String> {
    if array.is_null(row) {
        return Ok(String::new());
    }
    match csv_non_null(array, row, binary_format) {
        Ok(s) => Ok(s),
        Err(Error::UnsupportedCsvType { .. }) => {
            let v = json_non_null(array, row, binary_format)?;
            Ok(v.to_string())
        }
        Err(e) => Err(e),
    }
}

// ---------- JSON value formatting ----------

/// Format a single cell as a `serde_json::Value`.
pub fn json_value(array: &dyn Array, row: usize, binary_format: BinaryFormat) -> Result<Value> {
    if array.is_null(row) {
        return Ok(Value::Null);
    }
    json_non_null(array, row, binary_format)
}

fn json_non_null(array: &dyn Array, row: usize, binary_format: BinaryFormat) -> Result<Value> {
    use DataType::*;
    match array.data_type() {
        Null => Ok(Value::Null),
        Boolean => Ok(Value::Bool(array.as_boolean().value(row))),
        Int8 => Ok(Value::from(array.as_primitive::<Int8Type>().value(row))),
        Int16 => Ok(Value::from(array.as_primitive::<Int16Type>().value(row))),
        Int32 => Ok(Value::from(array.as_primitive::<Int32Type>().value(row))),
        Int64 => Ok(Value::from(array.as_primitive::<Int64Type>().value(row))),
        UInt8 => Ok(Value::from(array.as_primitive::<UInt8Type>().value(row))),
        UInt16 => Ok(Value::from(array.as_primitive::<UInt16Type>().value(row))),
        UInt32 => Ok(Value::from(array.as_primitive::<UInt32Type>().value(row))),
        UInt64 => Ok(Value::from(array.as_primitive::<UInt64Type>().value(row))),
        Float16 => Ok(float_json(f64::from(f32::from(
            array.as_primitive::<Float16Type>().value(row),
        )))),
        Float32 => Ok(float_json(f64::from(
            array.as_primitive::<Float32Type>().value(row),
        ))),
        Float64 => Ok(float_json(array.as_primitive::<Float64Type>().value(row))),
        Utf8 => Ok(Value::String(
            array.as_string::<i32>().value(row).to_string(),
        )),
        LargeUtf8 => Ok(Value::String(
            array.as_string::<i64>().value(row).to_string(),
        )),
        Utf8View => Ok(Value::String(array.as_string_view().value(row).to_string())),
        Binary => Ok(encode_binary_json(
            array.as_binary::<i32>().value(row),
            binary_format,
        )),
        LargeBinary => Ok(encode_binary_json(
            array.as_binary::<i64>().value(row),
            binary_format,
        )),
        BinaryView => Ok(encode_binary_json(
            array.as_binary_view().value(row),
            binary_format,
        )),
        FixedSizeBinary(_) => Ok(encode_binary_json(
            array.as_fixed_size_binary().value(row),
            binary_format,
        )),
        Date32 => Ok(Value::String(format_date32(
            array
                .as_any()
                .downcast_ref::<Date32Array>()
                .unwrap()
                .value(row),
        ))),
        Date64 => Ok(Value::String(format_date64(
            array
                .as_any()
                .downcast_ref::<Date64Array>()
                .unwrap()
                .value(row),
        ))),
        Time32(unit) => Ok(Value::String(format_time32(
            time32_value_at(array, *unit, row),
            *unit,
        ))),
        Time64(unit) => {
            let v = match unit {
                TimeUnit::Microsecond => array.as_primitive::<Time64MicrosecondType>().value(row),
                TimeUnit::Nanosecond => array.as_primitive::<Time64NanosecondType>().value(row),
                TimeUnit::Second | TimeUnit::Millisecond => {
                    unreachable!("arrow disallows Time64 with second/millisecond units")
                }
            };
            Ok(Value::String(format_time64(v, *unit)))
        }
        Timestamp(unit, tz) => Ok(Value::String(format_timestamp_at(
            array,
            *unit,
            tz.as_deref(),
            row,
        ))),
        Duration(unit) => Ok(Value::String(format_duration_at(array, *unit, row))),
        Interval(unit) => {
            use arrow_schema::IntervalUnit::*;
            let s = match unit {
                YearMonth => {
                    let v = array.as_primitive::<IntervalYearMonthType>().value(row);
                    format_interval_year_month(v)
                }
                DayTime => {
                    let v = array.as_primitive::<IntervalDayTimeType>().value(row);
                    format_interval_day_time(v.days, v.milliseconds)
                }
                MonthDayNano => {
                    let v = array.as_primitive::<IntervalMonthDayNanoType>().value(row);
                    format_interval_month_day_nano(v.months, v.days, v.nanoseconds)
                }
            };
            Ok(Value::String(s))
        }
        Decimal32(_, _) => {
            let v = array.as_primitive::<Decimal32Type>().value(row);
            Ok(json_number_from_str(&v.to_string()))
        }
        Decimal64(_, _) => {
            let v = array.as_primitive::<Decimal64Type>().value(row);
            Ok(json_number_from_str(&v.to_string()))
        }
        Decimal128(_, _) => {
            let v = array.as_primitive::<Decimal128Type>().value(row);
            Ok(json_number_from_str(&v.to_string()))
        }
        Decimal256(_, _) => {
            let v = array.as_primitive::<Decimal256Type>().value(row);
            Ok(json_number_from_str(&v.to_string()))
        }
        List(_) => json_list_like(array.as_list::<i32>().value(row).as_ref(), binary_format),
        LargeList(_) => json_list_like(array.as_list::<i64>().value(row).as_ref(), binary_format),
        FixedSizeList(_, _) => json_list_like(
            array.as_fixed_size_list().value(row).as_ref(),
            binary_format,
        ),
        Struct(_) => {
            let s = array.as_struct();
            let mut obj = JsonMap::new();
            for (i, field) in s.fields().iter().enumerate() {
                let child = s.column(i);
                obj.insert(
                    field.name().clone(),
                    json_value(child.as_ref(), row, binary_format)?,
                );
            }
            Ok(Value::Object(obj))
        }
        Map(_, _) => {
            let m = array.as_map();
            let start = m.value_offsets()[row] as usize;
            let end = m.value_offsets()[row + 1] as usize;
            let keys = m.keys();
            let values = m.values();
            let mut obj = JsonMap::new();
            for i in start..end {
                let key = json_value(keys.as_ref(), i, binary_format)?;
                let key_str = match key {
                    Value::String(s) => s,
                    other => other.to_string(),
                };
                let val = json_value(values.as_ref(), i, binary_format)?;
                obj.insert(key_str, val);
            }
            Ok(Value::Object(obj))
        }
        Dictionary(key_ty, _) => {
            let values = dict_values(array, key_ty);
            let logical_index = dict_logical_index(array, key_ty, row);
            json_value(values.as_ref(), logical_index, binary_format)
        }
        other => Err(Error::UnsupportedCsvType {
            column: String::new(),
            data_type: format!("unsupported arrow type in JSONL output: {other:?}"),
        }),
    }
}

fn json_list_like(array: &dyn Array, binary_format: BinaryFormat) -> Result<Value> {
    let mut out = Vec::with_capacity(array.len());
    for i in 0..array.len() {
        out.push(json_value(array, i, binary_format)?);
    }
    Ok(Value::Array(out))
}

// ---------- helpers ----------

fn float_json(v: f64) -> Value {
    if v.is_nan() {
        Value::String("NaN".to_string())
    } else if v.is_infinite() {
        Value::String(if v > 0.0 { "Infinity" } else { "-Infinity" }.to_string())
    } else {
        serde_json::Number::from_f64(v)
            .map(Value::Number)
            .unwrap_or(Value::Null)
    }
}

fn format_f32_csv(v: f32) -> String {
    if v.is_nan() {
        "NaN".into()
    } else if v.is_infinite() {
        if v > 0.0 { "inf".into() } else { "-inf".into() }
    } else {
        v.to_string()
    }
}

fn format_f64_csv(v: f64) -> String {
    if v.is_nan() {
        "NaN".into()
    } else if v.is_infinite() {
        if v > 0.0 { "inf".into() } else { "-inf".into() }
    } else {
        v.to_string()
    }
}

fn hex_escape(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 4);
    for b in bytes {
        s.push('\\');
        s.push('x');
        s.push(hex_digit(b >> 4));
        s.push(hex_digit(b & 0x0f));
    }
    s
}

/// Literal placeholder used in CSV and JSONL when `--binary-format none` is set.
pub const BINARY_PLACEHOLDER: &str = "BINARY_DATA";

/// CSV cell encoding for a binary value.
fn encode_binary(bytes: &[u8], binary_format: BinaryFormat) -> String {
    match binary_format {
        BinaryFormat::None => BINARY_PLACEHOLDER.to_string(),
        BinaryFormat::Hex => hex_escape(bytes),
        BinaryFormat::Base64 => BASE64.encode(bytes),
    }
}

/// JSON-value encoding for a binary value.
fn encode_binary_json(bytes: &[u8], binary_format: BinaryFormat) -> Value {
    match binary_format {
        BinaryFormat::None => Value::String(BINARY_PLACEHOLDER.to_string()),
        BinaryFormat::Hex => Value::String(hex_escape(bytes)),
        BinaryFormat::Base64 => Value::String(BASE64.encode(bytes)),
    }
}

fn hex_digit(n: u8) -> char {
    match n {
        0..=9 => (b'0' + n) as char,
        10..=15 => (b'a' + n - 10) as char,
        _ => unreachable!(),
    }
}

fn json_number_from_str(s: &str) -> Value {
    // With `arbitrary_precision`, Number can hold any decimal string.
    match s.parse::<serde_json::Number>() {
        Ok(n) => Value::Number(n),
        Err(_) => Value::String(s.to_string()),
    }
}

// ---------- date / time / timestamp ----------

fn format_date32(days: i32) -> String {
    epoch_date()
        .checked_add_signed(chrono::Duration::days(i64::from(days)))
        .map_or_else(|| format!("{days}"), |d| d.format("%Y-%m-%d").to_string())
}

fn format_date64(ms: i64) -> String {
    DateTime::<chrono::Utc>::from_timestamp_millis(ms)
        .map_or_else(|| format!("{ms}"), |d| d.format("%Y-%m-%d").to_string())
}

fn format_time32(v: i32, unit: TimeUnit) -> String {
    let (secs, nanos) = match unit {
        TimeUnit::Second => (i64::from(v), 0),
        TimeUnit::Millisecond => {
            let v64 = i64::from(v);
            (v64 / 1_000, (v64 % 1_000) * 1_000_000)
        }
        _ => unreachable!("Time32 only supports Second / Millisecond"),
    };
    format_time_parts(secs, nanos.try_into().unwrap_or(0), unit)
}

fn format_time64(v: i64, unit: TimeUnit) -> String {
    let (secs, nanos) = match unit {
        TimeUnit::Microsecond => (v / 1_000_000, (v % 1_000_000) * 1_000),
        TimeUnit::Nanosecond => (v / 1_000_000_000, v % 1_000_000_000),
        _ => unreachable!("Time64 only supports Microsecond / Nanosecond"),
    };
    format_time_parts(secs, nanos.try_into().unwrap_or(0), unit)
}

fn format_time_parts(secs: i64, nanos: u32, unit: TimeUnit) -> String {
    let total = (secs.rem_euclid(86_400)) as u32;
    let h = total / 3600;
    let m = (total / 60) % 60;
    let s = total % 60;
    let t = NaiveTime::from_hms_nano_opt(h, m, s, nanos).unwrap_or_default();
    match unit {
        TimeUnit::Second => t.format("%H:%M:%S").to_string(),
        TimeUnit::Millisecond => t.format("%H:%M:%S%.3f").to_string(),
        TimeUnit::Microsecond => t.format("%H:%M:%S%.6f").to_string(),
        TimeUnit::Nanosecond => t.format("%H:%M:%S%.9f").to_string(),
    }
}

fn epoch_date() -> NaiveDate {
    NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch")
}

fn time32_value_at(array: &dyn Array, unit: TimeUnit, row: usize) -> i32 {
    match unit {
        TimeUnit::Second => array.as_primitive::<Time32SecondType>().value(row),
        TimeUnit::Millisecond => array.as_primitive::<Time32MillisecondType>().value(row),
        _ => unreachable!("Time32 only supports Second / Millisecond"),
    }
}

fn timestamp_value_at(array: &dyn Array, unit: TimeUnit, row: usize) -> i64 {
    match unit {
        TimeUnit::Second => array.as_primitive::<TimestampSecondType>().value(row),
        TimeUnit::Millisecond => array.as_primitive::<TimestampMillisecondType>().value(row),
        TimeUnit::Microsecond => array.as_primitive::<TimestampMicrosecondType>().value(row),
        TimeUnit::Nanosecond => array.as_primitive::<TimestampNanosecondType>().value(row),
    }
}

fn format_timestamp_at(array: &dyn Array, unit: TimeUnit, tz: Option<&str>, row: usize) -> String {
    let v = timestamp_value_at(array, unit, row);
    let (secs, nanos) = decompose_timestamp(v, unit);
    match tz {
        None => naive_ts_iso(secs, nanos, unit),
        Some(tz_str) => zoned_ts_iso(secs, nanos, unit, tz_str),
    }
}

fn decompose_timestamp(v: i64, unit: TimeUnit) -> (i64, u32) {
    match unit {
        TimeUnit::Second => (v, 0),
        TimeUnit::Millisecond => (
            v.div_euclid(1_000),
            (v.rem_euclid(1_000) * 1_000_000) as u32,
        ),
        TimeUnit::Microsecond => (
            v.div_euclid(1_000_000),
            (v.rem_euclid(1_000_000) * 1_000) as u32,
        ),
        TimeUnit::Nanosecond => (
            v.div_euclid(1_000_000_000),
            v.rem_euclid(1_000_000_000) as u32,
        ),
    }
}

fn naive_ts_iso(secs: i64, nanos: u32, unit: TimeUnit) -> String {
    let Some(dt) = DateTime::<chrono::Utc>::from_timestamp(secs, nanos) else {
        return format!("{secs}.{nanos:09}");
    };
    let naive: NaiveDateTime = dt.naive_utc();
    match unit {
        TimeUnit::Second => naive.format("%Y-%m-%dT%H:%M:%S").to_string(),
        TimeUnit::Millisecond => naive.format("%Y-%m-%dT%H:%M:%S%.3f").to_string(),
        TimeUnit::Microsecond => naive.format("%Y-%m-%dT%H:%M:%S%.6f").to_string(),
        TimeUnit::Nanosecond => naive.format("%Y-%m-%dT%H:%M:%S%.9f").to_string(),
    }
}

fn zoned_ts_iso(secs: i64, nanos: u32, unit: TimeUnit, tz: &str) -> String {
    let Some(utc) = DateTime::<chrono::Utc>::from_timestamp(secs, nanos) else {
        return format!("{secs}.{nanos:09}{tz}");
    };
    // Best-effort: try to parse tz as a fixed offset like "+02:00" or "-05:30".
    // If it is a named zone (e.g. "America/New_York"), fall back to emitting as-is.
    if let Some(offset) = parse_fixed_offset(tz) {
        let dt = utc.with_timezone(&offset);
        return match unit {
            TimeUnit::Second => dt.format("%Y-%m-%dT%H:%M:%S%:z").to_string(),
            TimeUnit::Millisecond => dt.format("%Y-%m-%dT%H:%M:%S%.3f%:z").to_string(),
            TimeUnit::Microsecond => dt.format("%Y-%m-%dT%H:%M:%S%.6f%:z").to_string(),
            TimeUnit::Nanosecond => dt.format("%Y-%m-%dT%H:%M:%S%.9f%:z").to_string(),
        };
    }
    // Named zones: emit UTC instant with explicit Z and a trailing "[<zone>]"
    // marker so information is preserved without pulling in tzdata.
    let base = naive_ts_iso(secs, nanos, unit);
    format!("{base}Z[{tz}]")
}

fn parse_fixed_offset(s: &str) -> Option<FixedOffset> {
    if s == "Z" || s == "UTC" || s == "+00:00" || s == "-00:00" {
        return FixedOffset::east_opt(0);
    }
    // Accept "+HH:MM", "-HH:MM", "+HHMM", "-HHMM".
    let bytes = s.as_bytes();
    if bytes.len() < 3 {
        return None;
    }
    let sign = match bytes[0] {
        b'+' => 1,
        b'-' => -1,
        _ => return None,
    };
    let rest = &s[1..];
    let (hh, mm) = if let Some((h, m)) = rest.split_once(':') {
        (h, m)
    } else if rest.len() == 4 {
        (&rest[..2], &rest[2..])
    } else {
        return None;
    };
    let h: i32 = hh.parse().ok()?;
    let m: i32 = mm.parse().ok()?;
    FixedOffset::east_opt(sign * (h * 3600 + m * 60))
}

// ---------- duration / interval ----------

fn duration_value_at(array: &dyn Array, unit: TimeUnit, row: usize) -> i64 {
    match unit {
        TimeUnit::Second => array.as_primitive::<DurationSecondType>().value(row),
        TimeUnit::Millisecond => array.as_primitive::<DurationMillisecondType>().value(row),
        TimeUnit::Microsecond => array.as_primitive::<DurationMicrosecondType>().value(row),
        TimeUnit::Nanosecond => array.as_primitive::<DurationNanosecondType>().value(row),
    }
}

fn format_duration_at(array: &dyn Array, unit: TimeUnit, row: usize) -> String {
    let v = duration_value_at(array, unit, row);
    let (secs, nanos) = match unit {
        TimeUnit::Second => (v, 0i64),
        TimeUnit::Millisecond => (v.div_euclid(1_000), v.rem_euclid(1_000) * 1_000_000),
        TimeUnit::Microsecond => (v.div_euclid(1_000_000), v.rem_euclid(1_000_000) * 1_000),
        TimeUnit::Nanosecond => (v.div_euclid(1_000_000_000), v.rem_euclid(1_000_000_000)),
    };
    let sign = if secs < 0 || nanos < 0 { "-" } else { "" };
    let a_secs = secs.unsigned_abs();
    let a_nanos = nanos.unsigned_abs();
    let hours = a_secs / 3600;
    let minutes = (a_secs % 3600) / 60;
    let seconds = a_secs % 60;
    let mut buf = format!("{sign}PT");
    if hours > 0 {
        buf.push_str(&format!("{hours}H"));
    }
    if minutes > 0 {
        buf.push_str(&format!("{minutes}M"));
    }
    if a_nanos > 0 {
        let frac = format!("{a_nanos:09}");
        let trimmed = frac.trim_end_matches('0');
        buf.push_str(&format!("{seconds}.{trimmed}S"));
    } else if seconds > 0 || (hours == 0 && minutes == 0) {
        buf.push_str(&format!("{seconds}S"));
    }
    buf
}

fn format_interval_year_month(months: i32) -> String {
    let sign = if months < 0 { "-" } else { "" };
    let a = months.unsigned_abs();
    let y = a / 12;
    let m = a % 12;
    if y > 0 && m > 0 {
        format!("{sign}P{y}Y{m}M")
    } else if y > 0 {
        format!("{sign}P{y}Y")
    } else {
        format!("{sign}P{m}M")
    }
}

fn format_interval_day_time(days: i32, ms: i32) -> String {
    let sign_bit = days < 0 || ms < 0;
    let sign = if sign_bit { "-" } else { "" };
    let d = days.unsigned_abs();
    let ms_abs = ms.unsigned_abs();
    let s = ms_abs / 1000;
    let frac = ms_abs % 1000;
    let mut out = format!("{sign}P");
    if d > 0 {
        out.push_str(&format!("{d}D"));
    }
    if s > 0 || frac > 0 {
        out.push('T');
        if frac == 0 {
            out.push_str(&format!("{s}S"));
        } else {
            out.push_str(&format!("{s}.{frac:03}S"));
        }
    }
    if out == "P" || out == "-P" {
        out.push_str("T0S");
    }
    out
}

fn format_interval_month_day_nano(months: i32, days: i32, nanos: i64) -> String {
    let sign_bit = months < 0 || days < 0 || nanos < 0;
    let sign = if sign_bit { "-" } else { "" };
    let mo = months.unsigned_abs();
    let d = days.unsigned_abs();
    let n = nanos.unsigned_abs();
    let s = n / 1_000_000_000;
    let frac = n % 1_000_000_000;
    let mut out = format!("{sign}P");
    if mo > 0 {
        out.push_str(&format!("{mo}M"));
    }
    if d > 0 {
        out.push_str(&format!("{d}D"));
    }
    if s > 0 || frac > 0 {
        out.push('T');
        if frac == 0 {
            out.push_str(&format!("{s}S"));
        } else {
            let f = format!("{frac:09}");
            let trimmed = f.trim_end_matches('0');
            out.push_str(&format!("{s}.{trimmed}S"));
        }
    }
    if out == "P" || out == "-P" {
        out.push_str("T0S");
    }
    out
}

// ---------- decimals ----------

fn format_decimal128_csv(v: i128, scale: i8) -> String {
    insert_decimal_point(&v.to_string(), scale)
}

fn format_decimal256_csv(raw: String, scale: i8) -> String {
    insert_decimal_point(&raw, scale)
}

fn insert_decimal_point(raw: &str, scale: i8) -> String {
    if scale <= 0 {
        if scale == 0 {
            return raw.to_string();
        }
        // Negative scale means raw * 10^(-scale).
        let mut s = raw.to_string();
        s.extend(std::iter::repeat_n('0', (-scale) as usize));
        return s;
    }
    let scale = scale as usize;
    let (sign, digits) = match raw.strip_prefix('-') {
        Some(rest) => ("-", rest),
        None => ("", raw),
    };
    if digits.len() <= scale {
        let pad = scale - digits.len();
        let zeros = "0".repeat(pad);
        return format!("{sign}0.{zeros}{digits}");
    }
    let split = digits.len() - scale;
    let (int_part, frac_part) = digits.split_at(split);
    format!("{sign}{int_part}.{frac_part}")
}

// ---------- dictionary helpers ----------

fn dict_values(array: &dyn Array, key_ty: &DataType) -> Arc<dyn Array> {
    use DataType::*;
    match key_ty {
        Int8 => as_dictionary_array::<Int8Type>(array).values().clone(),
        Int16 => as_dictionary_array::<Int16Type>(array).values().clone(),
        Int32 => as_dictionary_array::<Int32Type>(array).values().clone(),
        Int64 => as_dictionary_array::<Int64Type>(array).values().clone(),
        UInt8 => as_dictionary_array::<UInt8Type>(array).values().clone(),
        UInt16 => as_dictionary_array::<UInt16Type>(array).values().clone(),
        UInt32 => as_dictionary_array::<UInt32Type>(array).values().clone(),
        UInt64 => as_dictionary_array::<UInt64Type>(array).values().clone(),
        _ => unreachable!("unsupported dictionary key type"),
    }
}

fn dict_logical_index(array: &dyn Array, key_ty: &DataType, row: usize) -> usize {
    use DataType::*;
    match key_ty {
        Int8 => as_dictionary_array::<Int8Type>(array).keys().value(row) as usize,
        Int16 => as_dictionary_array::<Int16Type>(array).keys().value(row) as usize,
        Int32 => as_dictionary_array::<Int32Type>(array).keys().value(row) as usize,
        Int64 => as_dictionary_array::<Int64Type>(array).keys().value(row) as usize,
        UInt8 => as_dictionary_array::<UInt8Type>(array).keys().value(row) as usize,
        UInt16 => as_dictionary_array::<UInt16Type>(array).keys().value(row) as usize,
        UInt32 => as_dictionary_array::<UInt32Type>(array).keys().value(row) as usize,
        UInt64 => as_dictionary_array::<UInt64Type>(array).keys().value(row) as usize,
        _ => unreachable!("unsupported dictionary key type"),
    }
}
