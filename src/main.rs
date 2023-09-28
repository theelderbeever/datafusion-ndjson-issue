use datafusion::arrow::{record_batch::RecordBatch, util::pretty::pretty_format_batches};
use datafusion::{
    arrow::datatypes::{DataType, TimeUnit},
    prelude::{NdJsonReadOptions, ParquetReadOptions, SessionConfig, SessionContext},
};

#[tokio::main]
async fn main() {
    env_logger::init();
    let table = std::env::args().nth(1).unwrap();
    let config = SessionConfig::new().with_information_schema(true);
    let ctx = SessionContext::with_config(config);

    ctx.register_json(
        "ndjson",
        "data/ndjson",
        NdJsonReadOptions::default()
            .table_partition_cols(vec![(
                "hourly_timestamp".to_string(),
                DataType::Timestamp(TimeUnit::Second, None),
            )])
            .file_extension(".ndjson"),
    )
    .await
    .unwrap();

    ctx.register_parquet(
        "parquet",
        "data/parquet",
        ParquetReadOptions::default().table_partition_cols(vec![(
            "hourly_timestamp".to_string(),
            DataType::Timestamp(TimeUnit::Second, None),
        )]),
    )
    .await
    .unwrap();

    display_query_result(&ctx, "show tables").await;
    display_query_result(&ctx, &format!("show columns from {table}")).await;

    display_query_result(
        &ctx,
        &format!("SELECT * FROM {table} WHERE hourly_timestamp = '2023-09-25T20:00:00'"),
    )
    .await;
}

async fn display_query_result(ctx: &SessionContext, query: &str) {
    println!("{query}");
    let df = ctx.sql(query).await.unwrap();
    let results: Vec<RecordBatch> = df.collect().await.unwrap();

    println!("{}", pretty_format_batches(&results).unwrap());
}
