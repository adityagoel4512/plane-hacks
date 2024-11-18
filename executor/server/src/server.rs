use exec::evaluate;
use execserver::executor_service_server::{ExecutorService, ExecutorServiceServer};
use execserver::{ExpressionRequest, ExpressionResponse};
use tonic::{transport::Server, Request, Response, Status};

mod execserver {
    tonic::include_proto!("execserver");
}

#[derive(Debug, Default)]
struct ExecutorRpcServer {}

#[tonic::async_trait]
impl ExecutorService for ExecutorRpcServer {
    async fn execute_expression(
        &self,
        request: Request<ExpressionRequest>,
    ) -> Result<Response<ExpressionResponse>, Status> {
        let evaluated_result = evaluate(request.into_inner().expression)
            .map_err(|e| Status::aborted(e.to_string()))?;
        let expression_result = ExpressionResponse {
            result: format!("Your response is: {:?}", evaluated_result),
        };
        Ok(Response::new(expression_result))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse()?;
    let server = ExecutorRpcServer::default();
    Server::builder()
        .add_service(ExecutorServiceServer::new(server))
        .serve(addr)
        .await?;
    Ok(())
}
