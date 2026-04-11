use std::collections::HashMap;

use super::*;

// ---------------------------------------------------------------------------
// Expression evaluator
// ---------------------------------------------------------------------------

/// Evaluate an expression in the context of a row.
pub(crate) fn eval_expr(
    expr: &Expr,
    row: &Row,
    memgraph: &crate::graph::memgraph::MemGraph,
    params: &HashMap<String, Value>,
) -> Value {
    match expr {
        Expr::Integer(n) => Value::Int(*n),
        Expr::Float(f) => Value::Float(*f),
        Expr::StringLit(s) => Value::String(s.clone()),
        Expr::Bool(b) => Value::Bool(*b),
        Expr::Null => Value::Null,

        Expr::Ident(name) => row.get(name).cloned().unwrap_or(Value::Null),

        Expr::Parameter(name) => params.get(name).cloned().unwrap_or(Value::Null),

        Expr::PropertyAccess { object, property } => {
            let obj = eval_expr(object, row, memgraph, params);
            match obj {
                Value::Node(key) => {
                    if let Some(node) = memgraph.get_node(key) {
                        let prop_id = label_to_id(property.as_bytes());
                        for (pid, pval) in &node.properties {
                            if *pid == prop_id {
                                return property_value_to_value(pval);
                            }
                        }
                    }
                    Value::Null
                }
                Value::Edge(key) => {
                    if let Some(edge) = memgraph.get_edge(key) {
                        if let Some(props) = &edge.properties {
                            let prop_id = label_to_id(property.as_bytes());
                            for (pid, pval) in props {
                                if *pid == prop_id {
                                    return property_value_to_value(pval);
                                }
                            }
                        }
                    }
                    Value::Null
                }
                Value::Map(entries) => {
                    for (k, v) in &entries {
                        if k == property {
                            return v.clone();
                        }
                    }
                    Value::Null
                }
                _ => Value::Null,
            }
        }

        Expr::BinaryOp { left, op, right } => {
            let lv = eval_expr(left, row, memgraph, params);
            let rv = eval_expr(right, row, memgraph, params);
            eval_binary_op(&lv, *op, &rv)
        }

        Expr::Not(inner) => {
            let v = eval_expr(inner, row, memgraph, params);
            match v {
                Value::Bool(b) => Value::Bool(!b),
                Value::Null => Value::Null,
                _ => Value::Null,
            }
        }

        Expr::Negate(inner) => {
            let v = eval_expr(inner, row, memgraph, params);
            match v {
                Value::Int(n) => Value::Int(-n),
                Value::Float(f) => Value::Float(-f),
                _ => Value::Null,
            }
        }

        Expr::IsNull { expr, negated } => {
            let v = eval_expr(expr, row, memgraph, params);
            let is_null = matches!(v, Value::Null);
            Value::Bool(if *negated { !is_null } else { is_null })
        }

        Expr::InList { expr, list } => {
            let val = eval_expr(expr, row, memgraph, params);
            let list_val = eval_expr(list, row, memgraph, params);
            match list_val {
                Value::List(items) => {
                    let found = items.iter().any(|item| {
                        matches!(compare_values(&val, item), std::cmp::Ordering::Equal)
                    });
                    Value::Bool(found)
                }
                _ => Value::Null,
            }
        }

        Expr::FunctionCall { name, args, .. } => {
            let lower_name = name.to_ascii_lowercase();
            match lower_name.as_str() {
                "id" => {
                    if let Some(arg) = args.first() {
                        let v = eval_expr(arg, row, memgraph, params);
                        match v {
                            Value::Node(k) => Value::Int(k.data().as_ffi() as i64),
                            Value::Edge(k) => Value::Int(k.data().as_ffi() as i64),
                            _ => Value::Null,
                        }
                    } else {
                        Value::Null
                    }
                }
                "labels" => {
                    if let Some(arg) = args.first() {
                        let v = eval_expr(arg, row, memgraph, params);
                        if let Value::Node(k) = v {
                            if let Some(node) = memgraph.get_node(k) {
                                let labels: Vec<Value> =
                                    node.labels.iter().map(|&l| Value::Int(l as i64)).collect();
                                return Value::List(labels);
                            }
                        }
                        Value::Null
                    } else {
                        Value::Null
                    }
                }
                "type" => {
                    if let Some(arg) = args.first() {
                        let v = eval_expr(arg, row, memgraph, params);
                        if let Value::Edge(k) = v {
                            if let Some(edge) = memgraph.get_edge(k) {
                                return Value::Int(edge.edge_type as i64);
                            }
                        }
                        Value::Null
                    } else {
                        Value::Null
                    }
                }
                "size" => {
                    if let Some(arg) = args.first() {
                        let v = eval_expr(arg, row, memgraph, params);
                        match v {
                            Value::List(items) => Value::Int(items.len() as i64),
                            Value::String(s) => Value::Int(s.len() as i64),
                            _ => Value::Null,
                        }
                    } else {
                        Value::Null
                    }
                }
                "tointeger" | "toint" => {
                    if let Some(arg) = args.first() {
                        let v = eval_expr(arg, row, memgraph, params);
                        match v {
                            Value::Int(n) => Value::Int(n),
                            Value::Float(f) => Value::Int(f as i64),
                            Value::String(s) => s.parse::<i64>().map_or(Value::Null, Value::Int),
                            _ => Value::Null,
                        }
                    } else {
                        Value::Null
                    }
                }
                "tofloat" => {
                    if let Some(arg) = args.first() {
                        let v = eval_expr(arg, row, memgraph, params);
                        match v {
                            Value::Float(f) => Value::Float(f),
                            Value::Int(n) => Value::Float(n as f64),
                            Value::String(s) => s.parse::<f64>().map_or(Value::Null, Value::Float),
                            _ => Value::Null,
                        }
                    } else {
                        Value::Null
                    }
                }
                "tostring" => {
                    if let Some(arg) = args.first() {
                        let v = eval_expr(arg, row, memgraph, params);
                        Value::String(value_to_string(&v))
                    } else {
                        Value::Null
                    }
                }
                "count" | "collect" => {
                    // Aggregate functions are a no-op per-row; handled in
                    // the Project phase as a future enhancement. For now,
                    // return the value or null for count.
                    if let Some(arg) = args.first() {
                        eval_expr(arg, row, memgraph, params)
                    } else {
                        Value::Null
                    }
                }
                _ => Value::Null,
            }
        }

        Expr::List(items) => {
            let values: Vec<Value> = items
                .iter()
                .map(|item| eval_expr(item, row, memgraph, params))
                .collect();
            Value::List(values)
        }

        Expr::MapLit(entries) => {
            let map: Vec<(String, Value)> = entries
                .iter()
                .map(|(k, v)| (k.clone(), eval_expr(v, row, memgraph, params)))
                .collect();
            Value::Map(map)
        }

        Expr::Star => {
            let entries: Vec<(String, Value)> =
                row.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
            Value::Map(entries)
        }
    }
}

// ---------------------------------------------------------------------------
// Binary operator evaluation
// ---------------------------------------------------------------------------

pub(crate) fn eval_binary_op(left: &Value, op: BinaryOperator, right: &Value) -> Value {
    match op {
        // Logical operators
        BinaryOperator::And => match (left, right) {
            (Value::Bool(a), Value::Bool(b)) => Value::Bool(*a && *b),
            _ => Value::Null,
        },
        BinaryOperator::Or => match (left, right) {
            (Value::Bool(a), Value::Bool(b)) => Value::Bool(*a || *b),
            _ => Value::Null,
        },

        // Comparison operators
        BinaryOperator::Equal => {
            Value::Bool(compare_values(left, right) == std::cmp::Ordering::Equal)
        }
        BinaryOperator::NotEqual => {
            Value::Bool(compare_values(left, right) != std::cmp::Ordering::Equal)
        }
        BinaryOperator::LessThan => {
            Value::Bool(compare_values(left, right) == std::cmp::Ordering::Less)
        }
        BinaryOperator::GreaterThan => {
            Value::Bool(compare_values(left, right) == std::cmp::Ordering::Greater)
        }
        BinaryOperator::LessEqual => {
            let ord = compare_values(left, right);
            Value::Bool(ord == std::cmp::Ordering::Less || ord == std::cmp::Ordering::Equal)
        }
        BinaryOperator::GreaterEqual => {
            let ord = compare_values(left, right);
            Value::Bool(ord == std::cmp::Ordering::Greater || ord == std::cmp::Ordering::Equal)
        }

        // Arithmetic operators
        BinaryOperator::Add => match (left, right) {
            (Value::Int(a), Value::Int(b)) => Value::Int(a.wrapping_add(*b)),
            (Value::Float(a), Value::Float(b)) => Value::Float(a + b),
            (Value::Int(a), Value::Float(b)) => Value::Float(*a as f64 + b),
            (Value::Float(a), Value::Int(b)) => Value::Float(a + *b as f64),
            (Value::String(a), Value::String(b)) => {
                let mut s = a.clone();
                s.push_str(b);
                Value::String(s)
            }
            _ => Value::Null,
        },
        BinaryOperator::Sub => match (left, right) {
            (Value::Int(a), Value::Int(b)) => Value::Int(a.wrapping_sub(*b)),
            (Value::Float(a), Value::Float(b)) => Value::Float(a - b),
            (Value::Int(a), Value::Float(b)) => Value::Float(*a as f64 - b),
            (Value::Float(a), Value::Int(b)) => Value::Float(a - *b as f64),
            _ => Value::Null,
        },
        BinaryOperator::Mul => match (left, right) {
            (Value::Int(a), Value::Int(b)) => Value::Int(a.wrapping_mul(*b)),
            (Value::Float(a), Value::Float(b)) => Value::Float(a * b),
            (Value::Int(a), Value::Float(b)) => Value::Float(*a as f64 * b),
            (Value::Float(a), Value::Int(b)) => Value::Float(a * *b as f64),
            _ => Value::Null,
        },
        BinaryOperator::Div => match (left, right) {
            (Value::Int(a), Value::Int(b)) if *b != 0 => Value::Int(a / b),
            (Value::Float(a), Value::Float(b)) if *b != 0.0 => Value::Float(a / b),
            (Value::Int(a), Value::Float(b)) if *b != 0.0 => Value::Float(*a as f64 / b),
            (Value::Float(a), Value::Int(b)) if *b != 0 => Value::Float(a / *b as f64),
            _ => Value::Null,
        },
        BinaryOperator::Mod => match (left, right) {
            (Value::Int(a), Value::Int(b)) if *b != 0 => Value::Int(a % b),
            (Value::Float(a), Value::Float(b)) if *b != 0.0 => Value::Float(a % b),
            (Value::Int(a), Value::Float(b)) if *b != 0.0 => Value::Float(*a as f64 % b),
            (Value::Float(a), Value::Int(b)) if *b != 0 => Value::Float(a % *b as f64),
            _ => Value::Null,
        },

        BinaryOperator::RegexMatch => {
            // Cypher `=~` operator: regex match against the text.
            // TODO: Add `regex` crate dependency for full regex support.
            // For now, support basic patterns: exact match, prefix (*suffix),
            // suffix (prefix*), and contains (*middle*).
            match (left, right) {
                (Value::String(text), Value::String(pattern)) => {
                    let matched = if let Some(stripped) = pattern.strip_prefix(".*") {
                        if let Some(middle) = stripped.strip_suffix(".*") {
                            text.contains(middle)
                        } else {
                            text.ends_with(stripped)
                        }
                    } else if let Some(stripped) = pattern.strip_suffix(".*") {
                        text.starts_with(stripped)
                    } else {
                        text == pattern
                    };
                    Value::Bool(matched)
                }
                _ => Value::Null,
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Value comparison (for Sort and equality)
// ---------------------------------------------------------------------------

/// Compare two Values for ordering.
/// NULL < Bool < Int/Float < String < Node < Edge < List < Map.
pub(crate) fn compare_values(a: &Value, b: &Value) -> std::cmp::Ordering {
    use std::cmp::Ordering;

    fn type_rank(v: &Value) -> u8 {
        match v {
            Value::Null => 0,
            Value::Bool(_) => 1,
            Value::Int(_) | Value::Float(_) => 2,
            Value::String(_) => 3,
            Value::Node(_) => 4,
            Value::Edge(_) => 5,
            Value::List(_) => 6,
            Value::Map(_) => 7,
        }
    }

    let ra = type_rank(a);
    let rb = type_rank(b);
    if ra != rb {
        return ra.cmp(&rb);
    }

    match (a, b) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
        // Numeric comparison with i64/f64 promotion.
        (Value::Int(a), Value::Int(b)) => a.cmp(b),
        (Value::Float(a), Value::Float(b)) => a.partial_cmp(b).unwrap_or(Ordering::Equal),
        (Value::Int(a), Value::Float(b)) => (*a as f64).partial_cmp(b).unwrap_or(Ordering::Equal),
        (Value::Float(a), Value::Int(b)) => a.partial_cmp(&(*b as f64)).unwrap_or(Ordering::Equal),
        (Value::String(a), Value::String(b)) => a.cmp(b),
        (Value::Node(a), Value::Node(b)) => a.data().as_ffi().cmp(&b.data().as_ffi()),
        (Value::Edge(a), Value::Edge(b)) => a.data().as_ffi().cmp(&b.data().as_ffi()),
        _ => Ordering::Equal,
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Convert a PropertyValue to a runtime Value.
pub(crate) fn property_value_to_value(pv: &PropertyValue) -> Value {
    match pv {
        PropertyValue::Int(n) => Value::Int(*n),
        PropertyValue::Float(f) => Value::Float(*f),
        PropertyValue::String(s) => Value::String(core::str::from_utf8(s).unwrap_or("").to_owned()),
        PropertyValue::Bool(b) => Value::Bool(*b),
        PropertyValue::Bytes(b) => Value::String(core::str::from_utf8(b).unwrap_or("").to_owned()),
    }
}

/// Convert a Value to a display string.
pub(crate) fn value_to_string(v: &Value) -> String {
    match v {
        Value::Null => "null".into(),
        Value::Int(n) => n.to_string(),
        Value::Float(f) => f.to_string(),
        Value::String(s) => s.clone(),
        Value::Bool(b) => b.to_string(),
        Value::Node(k) => format!("node:{}", k.data().as_ffi()),
        Value::Edge(k) => format!("edge:{}", k.data().as_ffi()),
        Value::List(items) => {
            let parts: Vec<String> = items.iter().map(value_to_string).collect();
            format!("[{}]", parts.join(", "))
        }
        Value::Map(entries) => {
            let parts: Vec<String> = entries
                .iter()
                .map(|(k, v)| format!("{}: {}", k, value_to_string(v)))
                .collect();
            format!("{{{}}}", parts.join(", "))
        }
    }
}

/// Convert an Expr to a display string (for column headers).
pub(crate) fn expr_to_string(expr: &Expr) -> String {
    match expr {
        Expr::Ident(name) => name.clone(),
        Expr::PropertyAccess { object, property } => {
            format!("{}.{}", expr_to_string(object), property)
        }
        Expr::FunctionCall { name, args, .. } => {
            let arg_strs: Vec<String> = args.iter().map(expr_to_string).collect();
            format!("{}({})", name, arg_strs.join(", "))
        }
        Expr::Star => "*".into(),
        Expr::Integer(n) => n.to_string(),
        Expr::Float(f) => f.to_string(),
        Expr::StringLit(s) => format!("'{s}'"),
        Expr::Bool(b) => b.to_string(),
        Expr::Null => "NULL".into(),
        _ => format!("{expr:?}"),
    }
}

/// Deduplicate projected rows (for DISTINCT).
pub(crate) fn dedup_rows(rows: &mut Vec<Vec<Value>>) {
    // Use string representation for dedup (simple approach).
    let mut seen = std::collections::HashSet::new();
    rows.retain(|row| {
        let key: Vec<String> = row.iter().map(value_to_string).collect();
        let key_str = key.join("|");
        seen.insert(key_str)
    });
}

/// Convert a runtime Value to a PropertyValue for storage.
pub(crate) fn value_to_property_value(v: &Value) -> Option<PropertyValue> {
    match v {
        Value::Int(n) => Some(PropertyValue::Int(*n)),
        Value::Float(f) => Some(PropertyValue::Float(*f)),
        Value::String(s) => Some(PropertyValue::String(Bytes::from(s.clone()))),
        Value::Bool(b) => Some(PropertyValue::Bool(*b)),
        _ => None,
    }
}
