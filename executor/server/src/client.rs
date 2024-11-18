use execserver::executor_service_client::ExecutorServiceClient;
use execserver::ExpressionRequest;

mod execserver {
    tonic::include_proto!("execserver");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let expression = std::env::args()
        .nth(1)
        .ok_or_else(|| "Provide expression")?;
    let mut client = ExecutorServiceClient::connect("http://[::1]:50051").await?;
    let request = tonic::Request::new(ExpressionRequest { expression });
    let response = client.execute_expression(request).await?;

    eprintln!("Response: {:?}", response);
    Ok(())
}
