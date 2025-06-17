use tonic::transport::Channel;

use crate::raft::consensus::Consensus;
use crate::raft::{consensus, proto, timer};
use super::logging::*;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex as TokioMutex;

// RPC Server
#[derive(Clone)]
pub struct Server {
    pub consensus: Arc<TokioMutex<consensus::Consensus>>,
}

// #[tokio::main]
pub async fn start_server(
    addr: &str,
    consensus: Arc<TokioMutex<Consensus>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr = addr.parse().unwrap();

    info!("Raft server listening on {}", addr);

    let consensus_server = Server {
        consensus: consensus.clone(),
    };
    let management_server = Server {
        consensus: consensus.clone(),
    };
    tonic::transport::Server::builder()
        .add_service(proto::consensus_rpc_server::ConsensusRpcServer::new(
            consensus_server,
        ))
        .add_service(proto::management_rpc_server::ManagementRpcServer::new(
            management_server,
        ))
        .serve(addr)
        .await?;

    Ok(())
}

#[tonic::async_trait]
impl proto::consensus_rpc_server::ConsensusRpc for Server {
    async fn append_entries(
        &self,
        request: tonic::Request<proto::AppendEntriesRequest>,
    ) -> Result<tonic::Response<proto::AppendEntriesResponse>, tonic::Status> {
        let addr = request.remote_addr(); // Returns Option<SocketAddr>
        info!(
            "Handle append entries from {:?}, request: {:?}",
            &addr, &request
        );
        
        let mut consensus_guard = self.consensus.lock().await; // Lock TokioMutex
        let response_data = consensus_guard.handle_append_entries_rpc(request.get_ref()).await; // Pass &proto::AppendEntriesRequest
        
        let response = tonic::Response::new(response_data);
        info!(
            "Handle append entries from {:?}, response: {:?}",
            &addr, &response
        );
        Ok(response)
    }

    async fn request_vote(
        &self,
        request: tonic::Request<proto::RequestVoteRequest>,
    ) -> Result<tonic::Response<proto::RequestVoteResponse>, tonic::Status> {
        let addr = request.remote_addr();
        info!(
            "Handle request vote from {:?}, request: {:?}",
            &addr, &request
        );

        let mut consensus_guard = self.consensus.lock().await;
        let response_data = consensus_guard.handle_request_vote_rpc(request.get_ref()).await;
        
        let response = tonic::Response::new(response_data);
        info!(
            "Handle request vote from {:?}, response: {:?}",
            &addr, &response
        );
        Ok(response)
    }

    async fn install_snapshot(
        &self,
        request: tonic::Request<proto::InstallSnapshotRequest>,
    ) -> Result<tonic::Response<proto::InstallSnapshotResponse>, tonic::Status> {
        let addr = request.remote_addr();
        info!(
            "Handle install snapshot from {:?}, request: {:?}",
            &addr, &request
        );
        
        let mut consensus_guard = self.consensus.lock().await;
        let response_data = consensus_guard.handle_install_snapshot_rpc(request.get_ref()).await;

        let response = tonic::Response::new(response_data);
        info!(
            "Handle install snapshot from {:?}, response: {:?}",
            &addr, &response
        );
        Ok(response)
    }
}

#[tonic::async_trait]
impl proto::management_rpc_server::ManagementRpc for Server {
    async fn get_leader(
        &self,
        request: tonic::Request<proto::GetLeaderRequest>,
    ) -> Result<tonic::Response<proto::GetLeaderResponse>, tonic::Status> {
        let addr = request.remote_addr();
        info!(
            "Handle get leader from {:?}, request: {:?}",
            &addr, &request
        );

        let mut consensus_guard = self.consensus.lock().await;
        let response_data = consensus_guard.handle_get_leader_rpc(request.get_ref());
        
        let response = tonic::Response::new(response_data);
        info!(
            "Handle get leader from {:?}, response: {:?}",
            &addr, &response
        );
        Ok(response)
    }

    async fn get_configuration(
        &self,
        request: tonic::Request<proto::GetConfigurationRequest>,
    ) -> Result<tonic::Response<proto::GetConfigurationResponse>, tonic::Status> {
        let addr = request.remote_addr();
        info!(
            "Handle get configuration from {:?}, request: {:?}",
            &addr, &request
        );

        let mut consensus_guard = self.consensus.lock().await;
        let response_data = consensus_guard.handle_get_configuration_rpc(request.get_ref());

        let response = tonic::Response::new(response_data);
        info!(
            "Handle get configuration from {:?}, response: {:?}",
            &addr, &response
        );
        Ok(response)
    }

    async fn set_configuration(
        &self,
        request: tonic::Request<proto::SetConfigurationRequest>,
    ) -> Result<tonic::Response<proto::SetConfigurationResponse>, tonic::Status> {
        // No longer need spawn_blocking for this specific pattern
        let addr = request.remote_addr();
        info!(
            "Handle set configuration from {:?}, request: {:?}",
            &addr, &request
        );

        let mut consensus_guard = self.consensus.lock().await;
        let response_data = consensus_guard.handle_set_configuration_rpc(request.get_ref()).await;
        
        let response = tonic::Response::new(response_data);
        info!(
            "Handle set configuration from {:?}, response: {:?}",
            &addr, &response
        );
        Ok(response)
    }

    
    async fn propose(
        &self,
        request: tonic::Request<proto::ProposeRequest>,
    ) -> Result<tonic::Response<proto::ProposeResponse>, tonic::Status> {
        let addr = request.remote_addr();
        info!(
            "Handle propose from {:?}, request: {:?}",
            &addr, &request
        );

        let mut consensus_guard = self.consensus.lock().await;
        let response_data = consensus_guard.handle_propose_rpc(request.get_ref()).await;

        let response = tonic::Response::new(response_data);
        info!(
            "Handle propose from {:?}, response: {:?}",
            &addr, &response
        );
        Ok(response)
    }
    
}

#[derive(Debug, Clone)] 
pub struct Client {}

impl Client {
    pub async fn append_entries(
        &mut self, // If client is stateless, could be &self
        req: proto::AppendEntriesRequest,
        addr: String,
    ) -> Result<proto::AppendEntriesResponse, Box<dyn std::error::Error+Send+Sync>> {
        let addr_clone = addr.clone();
        let request_tonic = tonic::Request::new(req); // Renamed
        info!(
            "send rpc append_entries to {}, request: {:?}",
            &addr_clone, request_tonic
        );

        // Consider creating client once per peer and reusing, or using a connection pool
        let mut client = proto::consensus_rpc_client::ConsensusRpcClient::connect(format!("http://{}", addr)).await?;
        let response = client.append_entries(request_tonic).await?;
        info!(
            "send rpc append_entries to {}, response: {:?}",
            &addr_clone, response
        );

        Ok(response.into_inner())
    }

    pub async fn request_vote(
        &self,
        req: proto::RequestVoteRequest,
        addr: String,
    ) -> Result<proto::RequestVoteResponse, Box<dyn std::error::Error + Send+Sync>> {
        let addr_clone = addr.clone();
        let request_tonic = tonic::Request::new(req); // Renamed
        info!(
            "send rpc request_vote to {}, request: {:?}",
            &addr_clone, request_tonic
        );

        let mut client = proto::consensus_rpc_client::ConsensusRpcClient::connect(format!("http://{}", addr)).await?;
        let response = client.request_vote(request_tonic).await?;
        info!(
            "send rpc request_vote to {}, response: {:?}",
            &addr_clone, response
        );

        Ok(response.into_inner())
    }

    pub async fn install_snapshot(
        &mut self, // If client is stateless, could be &self
        req: proto::InstallSnapshotRequest,
        addr: String,
    ) -> Result<proto::InstallSnapshotResponse, Box<dyn std::error::Error+Send+Sync>> {
        let addr_clone = addr.clone();
        let request_tonic = tonic::Request::new(req); // Renamed
        info!(
            "send rpc install_snapshot to {}, request: {:?}",
            &addr_clone, request_tonic
        );

        let mut client = proto::consensus_rpc_client::ConsensusRpcClient::connect(format!("http://{}", addr)).await?;
        let response = client.install_snapshot(request_tonic).await?;
        info!(
            "send rpc install_snapshot to {}, response: {:?}",
            &addr_clone, response
        );

        Ok(response.into_inner())
    }

    pub async fn propose(
        &self,
        req: proto::ProposeRequest,
        addr: String,
    ) -> Result<proto::ProposeResponse, Box<dyn std::error::Error + Send + Sync>> {
        let mut client = proto::management_rpc_client::ManagementRpcClient::connect(format!("http://{}", addr)).await?;
        let response = client.propose(tonic::Request::new(req)).await?;
        Ok(response.into_inner())
    }

    /// 调用 Management RPC 的 GetLeader 方法
    pub async fn get_leader(
        &self, // 这个方法是无状态的，所以用 &self 即可
        req: proto::GetLeaderRequest,
        addr: String,
    ) -> Result<proto::GetLeaderResponse, Box<dyn std::error::Error + Send + Sync>> {
        // 注意：这里需要使用 ManagementRpcClient
        let mut client = proto::management_rpc_client::ManagementRpcClient::connect(format!("http://{}", addr)).await?;
        let response = client.get_leader(tonic::Request::new(req)).await?;
        Ok(response.into_inner())
    }

    /// 调用 Management RPC 的 GetConfiguration 方法
    pub async fn get_configuration(
        &self,
        req: proto::GetConfigurationRequest,
        addr: String,
    ) -> Result<proto::GetConfigurationResponse, Box<dyn std::error::Error + Send + Sync>> {
        let mut client = proto::management_rpc_client::ManagementRpcClient::connect(format!("http://{}", addr)).await?;
        let response = client.get_configuration(tonic::Request::new(req)).await?;
        Ok(response.into_inner())
    }

    /// 调用 Management RPC 的 SetConfiguration 方法
    pub async fn set_configuration(
        &self,
        req: proto::SetConfigurationRequest,
        addr: String,
    ) -> Result<proto::SetConfigurationResponse, Box<dyn std::error::Error + Send + Sync>> {
        let mut client = proto::management_rpc_client::ManagementRpcClient::connect(format!("http://{}", addr)).await?;
        let response = client.set_configuration(tonic::Request::new(req)).await?;
        Ok(response.into_inner())
    }
}