use std::{env, path::PathBuf};
fn main() {

    // 要给哪些东西派生？这其实是一个需要思考的问题
    
    tonic_build::configure()
        // 给proto生成的rust类型加上派生宏
        .type_attribute("LogEntry","#[derive(serde::Deserialize, serde::Serialize)]")
        .type_attribute("ServerInfo", "#[derive(serde::Deserialize, serde::Serialize)]")
        .compile_protos(&["proto/raft.proto"], &["proto"])
        .unwrap();


    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    
    tonic_build::configure()
        .file_descriptor_set_path(out_dir.join("helloworld_descriptor.bin"))
        .compile_protos(&["proto/helloworld.proto"], &["proto"])
        .unwrap();
}