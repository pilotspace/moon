//! Vector search example: create an index, ingest documents, KNN search.
//!
//! Run: `cargo run --example vector_search`

use moon_client::{
    DistanceMetric, MoonClient, Result, SchemaField, VectorIndexOptions, encode_vector,
};

const DIM: usize = 4; // toy dimensionality for the example

#[tokio::main]
async fn main() -> Result<()> {
    let mut client = MoonClient::connect("redis://127.0.0.1:6399").await?;
    let mut v = client.vector();

    // Clean up from previous runs
    let _ = v.drop_index("example_idx", true).await;

    // Create a HNSW vector index with an extra TAG field for filtering
    v.create_index(
        "example_idx",
        VectorIndexOptions::new(DIM, DistanceMetric::Cosine)
            .prefix("doc:")
            .m(16)
            .ef_construction(200)
            .compact_threshold(50)
            .add_field(SchemaField::Tag("category".into())),
    )
    .await?;
    println!("Index created.");

    // Insert documents (individual HSET calls trigger auto-indexing)
    let docs: &[(&str, &[f32], &str)] = &[
        ("doc:1", &[0.1, 0.2, 0.3, 0.4], "science"),
        ("doc:2", &[0.9, 0.8, 0.7, 0.6], "sports"),
        ("doc:3", &[0.15, 0.25, 0.35, 0.45], "science"),
        ("doc:4", &[0.5, 0.5, 0.5, 0.5], "politics"),
        ("doc:5", &[0.11, 0.22, 0.33, 0.44], "science"),
    ];

    for (key, vec, category) in docs {
        let blob = encode_vector(vec);
        client
            .hset_multiple(
                *key,
                &[("vec", blob.as_slice()), ("category", category.as_bytes())],
            )
            .await?;
    }
    println!("Inserted {} documents.", docs.len());

    // KNN search — find 3 most similar to doc:1's vector
    let query = [0.1_f32, 0.2, 0.3, 0.4];
    let results = v.search("example_idx", &query, 3).await?;
    println!("\nTop-3 KNN results for query [0.1, 0.2, 0.3, 0.4]:");
    for r in &results {
        println!("  key={} score={:.4} fields={:?}", r.key, r.score, r.fields);
    }

    // Index info
    let info = v.index_info("example_idx").await?;
    println!(
        "\nIndex info: num_docs={} dim={}",
        info.num_docs, info.dimension
    );

    // Compact the index
    let _ = v.compact("example_idx").await;
    println!("Compaction triggered.");

    // Cleanup
    v.drop_index("example_idx", true).await?;
    println!("\nIndex dropped, example complete.");

    Ok(())
}
