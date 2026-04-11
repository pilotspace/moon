//! Graph WAL serialization -- RESP-encoded graph commands for WAL replay.
//!
//! Graph operations serialize as standard RESP arrays, identical to how SET/GET
//! commands are stored in the WAL. This means zero WAL format changes -- graph
//! commands are just more RESP commands in the existing WAL block frames.
//!
//! Command format examples:
//! - `GRAPH.CREATE mygraph`
//! - `GRAPH.ADDNODE mygraph <node_id_u64> <num_labels> <label0> ... <num_props> <key0> <type0> <val0> ... [VECTOR <dim> <f32bytes>]`
//! - `GRAPH.ADDEDGE mygraph <edge_id_u64> <src_id_u64> <dst_id_u64> <edge_type_u16> <weight_f64> [<num_props> ...]`
//! - `GRAPH.REMOVENODE mygraph <node_id_u64>`
//! - `GRAPH.REMOVEEDGE mygraph <edge_id_u64>`
//! - `GRAPH.DROP mygraph`

use crate::graph::types::{PropertyMap, PropertyValue};

/// Format an f64 as a string. Uses Display formatting (no external crate needed).
/// This is only called during WAL serialization (cold path), so allocation is acceptable.
#[inline]
fn format_f64(val: f64) -> String {
    format!("{val}")
}

/// Write a RESP bulk string element: `$<len>\r\n<data>\r\n`
fn write_bulk(buf: &mut Vec<u8>, data: &[u8]) {
    buf.push(b'$');
    buf.extend_from_slice(itoa::Buffer::new().format(data.len()).as_bytes());
    buf.extend_from_slice(b"\r\n");
    buf.extend_from_slice(data);
    buf.extend_from_slice(b"\r\n");
}

/// Write a RESP array header: `*<count>\r\n`
fn write_array_header(buf: &mut Vec<u8>, count: usize) {
    buf.push(b'*');
    buf.extend_from_slice(itoa::Buffer::new().format(count).as_bytes());
    buf.extend_from_slice(b"\r\n");
}

/// Serialize `GRAPH.CREATE <graph_name>` as a RESP array.
pub fn serialize_graph_create(graph_name: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(64);
    write_array_header(&mut buf, 2);
    write_bulk(&mut buf, b"GRAPH.CREATE");
    write_bulk(&mut buf, graph_name);
    buf
}

/// Serialize `GRAPH.DROP <graph_name>` as a RESP array.
pub fn serialize_drop_graph(graph_name: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(64);
    write_array_header(&mut buf, 2);
    write_bulk(&mut buf, b"GRAPH.DROP");
    write_bulk(&mut buf, graph_name);
    buf
}

/// Serialize `GRAPH.REMOVENODE <graph_name> <node_id>` as a RESP array.
pub fn serialize_remove_node(graph_name: &[u8], node_id: u64) -> Vec<u8> {
    let mut buf = Vec::with_capacity(64);
    write_array_header(&mut buf, 3);
    write_bulk(&mut buf, b"GRAPH.REMOVENODE");
    write_bulk(&mut buf, graph_name);
    let id_str = itoa::Buffer::new().format(node_id).to_owned();
    write_bulk(&mut buf, id_str.as_bytes());
    buf
}

/// Serialize `GRAPH.REMOVEEDGE <graph_name> <edge_id>` as a RESP array.
pub fn serialize_remove_edge(graph_name: &[u8], edge_id: u64) -> Vec<u8> {
    let mut buf = Vec::with_capacity(64);
    write_array_header(&mut buf, 3);
    write_bulk(&mut buf, b"GRAPH.REMOVEEDGE");
    write_bulk(&mut buf, graph_name);
    let id_str = itoa::Buffer::new().format(edge_id).to_owned();
    write_bulk(&mut buf, id_str.as_bytes());
    buf
}

/// Serialize a GRAPH.ADDNODE command as a RESP array.
///
/// Format: `GRAPH.ADDNODE <graph> <node_id> <num_labels> [<label>...] <num_props> [<key> <type> <val>...] [VECTOR <dim> <f32bytes>]`
///
/// The `node_id` is the raw u64 representation of the SlotMap key (from `NodeKey::data().as_ffi()`).
pub fn serialize_add_node(
    graph_name: &[u8],
    node_id: u64,
    labels: &[u16],
    props: &PropertyMap,
    embedding: Option<&[f32]>,
) -> Vec<u8> {
    // Count elements: cmd + graph + node_id + num_labels + labels + num_props + props(key+type+val each) + optional VECTOR + dim + bytes
    let prop_elems = props.len() * 3; // key, type, value per prop
    let embed_elems = if embedding.is_some() { 3 } else { 0 }; // "VECTOR", dim, data
    let total = 4 + labels.len() + 1 + prop_elems + embed_elems;

    let mut buf = Vec::with_capacity(128 + prop_elems * 32);
    write_array_header(&mut buf, total);
    write_bulk(&mut buf, b"GRAPH.ADDNODE");
    write_bulk(&mut buf, graph_name);

    // Node ID
    let id_str = itoa::Buffer::new().format(node_id).to_owned();
    write_bulk(&mut buf, id_str.as_bytes());

    // Labels
    let nlabels_str = itoa::Buffer::new().format(labels.len()).to_owned();
    write_bulk(&mut buf, nlabels_str.as_bytes());
    for &label in labels {
        let label_str = itoa::Buffer::new().format(label).to_owned();
        write_bulk(&mut buf, label_str.as_bytes());
    }

    // Properties
    let nprops_str = itoa::Buffer::new().format(props.len()).to_owned();
    write_bulk(&mut buf, nprops_str.as_bytes());
    for (key, val) in props.iter() {
        let key_str = itoa::Buffer::new().format(*key).to_owned();
        write_bulk(&mut buf, key_str.as_bytes());
        serialize_property_value(&mut buf, val);
    }

    // Optional embedding
    if let Some(embed) = embedding {
        write_bulk(&mut buf, b"VECTOR");
        let dim_str = itoa::Buffer::new().format(embed.len()).to_owned();
        write_bulk(&mut buf, dim_str.as_bytes());
        // Encode as raw f32 bytes (little-endian)
        let bytes: Vec<u8> = embed.iter().flat_map(|f| f.to_le_bytes()).collect();
        write_bulk(&mut buf, &bytes);
    }

    buf
}

/// Serialize a GRAPH.ADDEDGE command as a RESP array.
///
/// Format: `GRAPH.ADDEDGE <graph> <edge_id> <src_id> <dst_id> <edge_type> <weight> <num_props> [<key> <type> <val>...]`
pub fn serialize_add_edge(
    graph_name: &[u8],
    edge_id: u64,
    src_id: u64,
    dst_id: u64,
    edge_type: u16,
    weight: f64,
    props: Option<&PropertyMap>,
) -> Vec<u8> {
    let prop_count = props.map_or(0, |p| p.len());
    let prop_elems = prop_count * 3;
    let total = 8 + prop_elems; // cmd + graph + edge_id + src + dst + type + weight + num_props + props

    let mut buf = Vec::with_capacity(128 + prop_elems * 32);
    write_array_header(&mut buf, total);
    write_bulk(&mut buf, b"GRAPH.ADDEDGE");
    write_bulk(&mut buf, graph_name);

    let edge_id_str = itoa::Buffer::new().format(edge_id).to_owned();
    write_bulk(&mut buf, edge_id_str.as_bytes());

    let src_str = itoa::Buffer::new().format(src_id).to_owned();
    write_bulk(&mut buf, src_str.as_bytes());

    let dst_str = itoa::Buffer::new().format(dst_id).to_owned();
    write_bulk(&mut buf, dst_str.as_bytes());

    let type_str = itoa::Buffer::new().format(edge_type).to_owned();
    write_bulk(&mut buf, type_str.as_bytes());

    // Weight as string representation
    let weight_str = format_f64(weight);
    write_bulk(&mut buf, weight_str.as_bytes());

    let nprops_str = itoa::Buffer::new().format(prop_count).to_owned();
    write_bulk(&mut buf, nprops_str.as_bytes());

    if let Some(props) = props {
        for (key, val) in props.iter() {
            let key_str = itoa::Buffer::new().format(*key).to_owned();
            write_bulk(&mut buf, key_str.as_bytes());
            serialize_property_value(&mut buf, val);
        }
    }

    buf
}

/// Serialize a property value as two RESP bulk strings: type tag + value.
fn serialize_property_value(buf: &mut Vec<u8>, val: &PropertyValue) {
    match val {
        PropertyValue::Int(i) => {
            write_bulk(buf, b"i");
            let s = itoa::Buffer::new().format(*i).to_owned();
            write_bulk(buf, s.as_bytes());
        }
        PropertyValue::Float(f) => {
            write_bulk(buf, b"f");
            let s = format_f64(*f);
            write_bulk(buf, s.as_bytes());
        }
        PropertyValue::String(s) => {
            write_bulk(buf, b"s");
            write_bulk(buf, s);
        }
        PropertyValue::Bool(b) => {
            write_bulk(buf, b"b");
            write_bulk(buf, if *b { b"1" } else { b"0" });
        }
        PropertyValue::Bytes(data) => {
            write_bulk(buf, b"x");
            write_bulk(buf, data);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use smallvec::smallvec;

    /// Parse a RESP array and return the elements as Vec<Vec<u8>>.
    fn parse_resp_array(data: &[u8]) -> Vec<Vec<u8>> {
        let mut pos = 0;
        assert_eq!(data[pos], b'*');
        pos += 1;

        // Read count
        let crlf = data[pos..].windows(2).position(|w| w == b"\r\n").unwrap();
        let count: usize = std::str::from_utf8(&data[pos..pos + crlf])
            .unwrap()
            .parse()
            .unwrap();
        pos += crlf + 2;

        let mut elements = Vec::with_capacity(count);
        for _ in 0..count {
            assert_eq!(data[pos], b'$');
            pos += 1;
            let crlf = data[pos..].windows(2).position(|w| w == b"\r\n").unwrap();
            let len: usize = std::str::from_utf8(&data[pos..pos + crlf])
                .unwrap()
                .parse()
                .unwrap();
            pos += crlf + 2;
            elements.push(data[pos..pos + len].to_vec());
            pos += len + 2; // skip \r\n
        }
        elements
    }

    #[test]
    fn test_serialize_graph_create() {
        let data = serialize_graph_create(b"social");
        let elems = parse_resp_array(&data);
        assert_eq!(elems.len(), 2);
        assert_eq!(elems[0], b"GRAPH.CREATE");
        assert_eq!(elems[1], b"social");
    }

    #[test]
    fn test_serialize_drop_graph() {
        let data = serialize_drop_graph(b"social");
        let elems = parse_resp_array(&data);
        assert_eq!(elems.len(), 2);
        assert_eq!(elems[0], b"GRAPH.DROP");
        assert_eq!(elems[1], b"social");
    }

    #[test]
    fn test_serialize_remove_node() {
        let data = serialize_remove_node(b"g", 42);
        let elems = parse_resp_array(&data);
        assert_eq!(elems.len(), 3);
        assert_eq!(elems[0], b"GRAPH.REMOVENODE");
        assert_eq!(elems[1], b"g");
        assert_eq!(elems[2], b"42");
    }

    #[test]
    fn test_serialize_remove_edge() {
        let data = serialize_remove_edge(b"g", 99);
        let elems = parse_resp_array(&data);
        assert_eq!(elems.len(), 3);
        assert_eq!(elems[0], b"GRAPH.REMOVEEDGE");
        assert_eq!(elems[1], b"g");
        assert_eq!(elems[2], b"99");
    }

    #[test]
    fn test_serialize_add_node_no_props_no_embedding() {
        let labels: &[u16] = &[1, 2];
        let props: PropertyMap = smallvec![];
        let data = serialize_add_node(b"social", 100, labels, &props, None);
        let elems = parse_resp_array(&data);
        // cmd + graph + node_id + num_labels(1) + 2 labels + num_props(1) = 7
        assert_eq!(elems[0], b"GRAPH.ADDNODE");
        assert_eq!(elems[1], b"social");
        assert_eq!(elems[2], b"100");
        assert_eq!(elems[3], b"2"); // num_labels
        assert_eq!(elems[4], b"1"); // label 0
        assert_eq!(elems[5], b"2"); // label 1
        assert_eq!(elems[6], b"0"); // num_props
    }

    #[test]
    fn test_serialize_add_node_with_props() {
        let labels: &[u16] = &[0];
        let props: PropertyMap = smallvec![
            (0, PropertyValue::Int(42)),
            (1, PropertyValue::String(Bytes::from_static(b"hello"))),
        ];
        let data = serialize_add_node(b"g", 1, labels, &props, None);
        let elems = parse_resp_array(&data);
        // cmd + graph + id + num_labels(1) + 1 label + num_props(1) + 2*3 props = 11
        assert_eq!(elems[0], b"GRAPH.ADDNODE");
        assert_eq!(elems[5], b"2"); // num_props
        // First prop: key=0, type=i, val=42
        assert_eq!(elems[6], b"0");
        assert_eq!(elems[7], b"i");
        assert_eq!(elems[8], b"42");
        // Second prop: key=1, type=s, val=hello
        assert_eq!(elems[9], b"1");
        assert_eq!(elems[10], b"s");
        assert_eq!(elems[11], b"hello");
    }

    #[test]
    fn test_serialize_add_node_with_embedding() {
        let labels: &[u16] = &[0];
        let props: PropertyMap = smallvec![];
        let embed = vec![1.0f32, 2.0, 3.0];
        let data = serialize_add_node(b"g", 1, labels, &props, Some(&embed));
        let elems = parse_resp_array(&data);
        // cmd + graph + id + num_labels + 1 label + num_props + VECTOR + dim + bytes = 10
        let n = elems.len();
        assert_eq!(elems[n - 3], b"VECTOR");
        assert_eq!(elems[n - 2], b"3"); // dim
        // Check the raw f32 bytes
        assert_eq!(elems[n - 1].len(), 12); // 3 * 4 bytes
    }

    #[test]
    fn test_serialize_add_edge_no_props() {
        let data = serialize_add_edge(b"social", 50, 10, 20, 3, 1.5, None);
        let elems = parse_resp_array(&data);
        assert_eq!(elems[0], b"GRAPH.ADDEDGE");
        assert_eq!(elems[1], b"social");
        assert_eq!(elems[2], b"50"); // edge_id
        assert_eq!(elems[3], b"10"); // src
        assert_eq!(elems[4], b"20"); // dst
        assert_eq!(elems[5], b"3"); // edge_type
        assert_eq!(elems[6], b"1.5"); // weight
        assert_eq!(elems[7], b"0"); // num_props
    }

    #[test]
    fn test_serialize_add_edge_with_props() {
        let props: PropertyMap = smallvec![(0, PropertyValue::Float(3.14))];
        let data = serialize_add_edge(b"g", 1, 2, 3, 0, 1.0, Some(&props));
        let elems = parse_resp_array(&data);
        assert_eq!(elems[7], b"1"); // num_props
        assert_eq!(elems[8], b"0"); // prop key
        assert_eq!(elems[9], b"f"); // type
        // ryu formats 3.14 as "3.14"
        assert_eq!(elems[10], b"3.14");
    }

    #[test]
    fn test_serialize_property_bool() {
        let props: PropertyMap = smallvec![(0, PropertyValue::Bool(true))];
        let data = serialize_add_node(b"g", 1, &[0], &props, None);
        let elems = parse_resp_array(&data);
        // After num_props at index 5: key=0, type=b, val=1
        assert_eq!(elems[6], b"0");
        assert_eq!(elems[7], b"b");
        assert_eq!(elems[8], b"1");
    }

    #[test]
    fn test_resp_format_is_valid() {
        // Verify that all serialized commands start with * and contain valid bulk strings
        let commands = vec![
            serialize_graph_create(b"test"),
            serialize_drop_graph(b"test"),
            serialize_remove_node(b"test", 1),
            serialize_remove_edge(b"test", 1),
            serialize_add_node(b"test", 1, &[0], &smallvec![], None),
            serialize_add_edge(b"test", 1, 2, 3, 0, 1.0, None),
        ];
        for cmd in commands {
            assert_eq!(cmd[0], b'*', "RESP array must start with *");
            // Should parse without panic
            let _ = parse_resp_array(&cmd);
        }
    }
}
