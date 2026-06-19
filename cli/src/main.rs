use clap::{Parser, Subcommand};
use ibcore::{
    AccountType, ConnectionState, DiagnosticEvent, FarmState, IbClient,
};
use std::collections::BTreeMap;
use std::time::Duration;

#[derive(Parser)]
#[command(
    name = "ibkr-diag",
    about = "Diagnostics CLI for IB Gateway health checks"
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Print version information for ibkr-diag, ibcore, and ibapi
    Version,
    /// Connect to IB Gateway and collect diagnostic events for a period
    Diagnose {
        /// IB Gateway host
        #[arg(short = 'H', long, default_value = "127.0.0.1")]
        host: String,

        /// IB Gateway port
        #[arg(short = 'P', long, default_value = "4002")]
        port: u16,

        /// Client ID
        #[arg(short = 'c', long, default_value = "1")]
        client_id: i32,

        /// Collection duration in seconds
        #[arg(short = 'd', long, default_value = "5")]
        duration: u64,

        /// Market data type: "delayed" or "realtime"
        #[arg(long, default_value = "delayed")]
        market_data: String,

        /// Output as JSON instead of text
        #[arg(long)]
        json: bool,
    },
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    match cli.command {
        Commands::Version => print_version(),
        Commands::Diagnose {
            host,
            port,
            client_id,
            duration,
            market_data,
            json,
        } => run_diagnose(host, port, client_id, duration, market_data, json).await,
    }
}

fn print_version() {
    println!("ibkr-diag {}", env!("CARGO_PKG_VERSION"));
    println!("ibcore    {}", ibcore::version());
    println!("ibapi     {}", ibcore::IBAPI_VERSION);
}

async fn run_diagnose(
    host: String,
    port: u16,
    client_id: i32,
    duration_secs: u64,
    market_data: String,
    json: bool,
) {
    let ib = match IbClient::connect(
        &host,
        port,
        client_id,
        &market_data,
        AccountType::Paper,
    )
    .await
    {
        Ok(client) => client,
        Err(e) => {
            eprintln!("Error: failed to connect to {host}:{port}: {e}");
            std::process::exit(1);
        }
    };

    let mut rx = ib.diagnostic_events();

    // Collect events for the specified duration
    let collect_duration = Duration::from_secs(duration_secs);
    let deadline = tokio::time::Instant::now() + collect_duration;

    let mut events: Vec<DiagnosticEvent> = Vec::new();
    loop {
        let timeout = tokio::time::sleep_until(deadline);
        tokio::select! {
            result = rx.recv() => {
                match result {
                    Ok(event) => events.push(event),
                    Err(_) => break, // channel lagged or closed
                }
            }
            _ = timeout => break,
        }
    }

    ib.disconnect().await;

    if json {
        print_json_report(&events, duration_secs);
    } else {
        print_text_report(&events, duration_secs);
    }
}

fn print_text_report(events: &[DiagnosticEvent], duration_secs: u64) {
    let total = events.len();
    let mut farm_counts: BTreeMap<String, usize> = BTreeMap::new();

    for event in events {
        let label = match event.farm_status {
            FarmState::Ok => "OK",
            FarmState::Warning => "Warning",
            FarmState::Inactive => "Inactive",
            FarmState::Unknown(_) => "Unknown",
        };
        *farm_counts.entry(label.to_string()).or_insert(0) += 1;
    }

    let gateway_version = events.first().map(|e| e.gateway_version).unwrap_or(0);
    let connection_state = events
        .first()
        .map(|e| match e.connection_state {
            ConnectionState::Connected => "connected",
            ConnectionState::Disconnected => "disconnected",
            ConnectionState::Reconnecting => "reconnecting",
        })
        .unwrap_or("unknown");
    let account_type = events
        .first()
        .map(|e| match e.account_type {
            AccountType::Live => "live",
            AccountType::Paper => "paper",
        })
        .unwrap_or("unknown");

    let ok_count = farm_counts.get("OK").copied().unwrap_or(0);
    let warning_count = farm_counts.get("Warning").copied().unwrap_or(0);
    let inactive_count = farm_counts.get("Inactive").copied().unwrap_or(0);
    let unknown_count = farm_counts.get("Unknown").copied().unwrap_or(0);

    println!("╔═══════════════════════════════════════════════╗");
    println!("║  IB Gateway Diagnostic Report                ║");
    println!("╠═══════════════════════════════════════════════╣");
    println!("║  Gateway version:  {:<32}║", gateway_version);
    println!("║  Connection state: {:<31}║", connection_state);
    println!("║  Account type:     {:<31}║", account_type);
    println!("║  Duration:         {:<4}s{:27}║", duration_secs, "");
    println!("║  Events received:  {:<4}{:27}║", total, "");
    println!("╠═══════════════════════════════════════════════╣");
    println!("║  Farm State Summary:                          ║");
    println!("║    OK:        {:<28}║", ok_count);
    println!("║    Warning:   {:<28}║", warning_count);
    println!("║    Inactive:  {:<28}║", inactive_count);
    println!("║    Unknown:   {:<28}║", unknown_count);
    println!("╚═══════════════════════════════════════════════╝");
}

fn print_json_report(events: &[DiagnosticEvent], duration_secs: u64) {
    let total = events.len();
    let mut farm_counts = serde_json::Map::new();

    let mut ok = 0usize;
    let mut warning = 0;
    let mut inactive = 0;
    let mut unknown = 0;

    for event in events {
        match event.farm_status {
            FarmState::Ok => ok += 1,
            FarmState::Warning => warning += 1,
            FarmState::Inactive => inactive += 1,
            FarmState::Unknown(_) => unknown += 1,
        }
    }
    farm_counts.insert("ok".to_string(), serde_json::json!(ok));
    farm_counts.insert("warning".to_string(), serde_json::json!(warning));
    farm_counts.insert("inactive".to_string(), serde_json::json!(inactive));
    farm_counts.insert("unknown".to_string(), serde_json::json!(unknown));

    let gateway_version = events.first().map(|e| e.gateway_version).unwrap_or(0);
    let connection_state = events
        .first()
        .map(|e| match e.connection_state {
            ConnectionState::Connected => "connected",
            ConnectionState::Disconnected => "disconnected",
            ConnectionState::Reconnecting => "reconnecting",
        })
        .unwrap_or("unknown");
    let account_type = events
        .first()
        .map(|e| match e.account_type {
            AccountType::Live => "live",
            AccountType::Paper => "paper",
        })
        .unwrap_or("unknown");

    let events_json: Vec<serde_json::Value> = events
        .iter()
        .map(|e| {
            serde_json::json!({
                "gateway_version": e.gateway_version,
                "error_code": e.error_code,
                "error_message": e.error_message,
                "farm_status": match e.farm_status {
                    FarmState::Ok => "ok",
                    FarmState::Warning => "warning",
                    FarmState::Inactive => "inactive",
                    FarmState::Unknown(_) => "unknown",
                },
                "connection_state": match e.connection_state {
                    ConnectionState::Connected => "connected",
                    ConnectionState::Disconnected => "disconnected",
                    ConnectionState::Reconnecting => "reconnecting",
                },
                "account_type": match e.account_type {
                    AccountType::Live => "live",
                    AccountType::Paper => "paper",
                },
            })
        })
        .collect();

    let report = serde_json::json!({
        "gateway_version": gateway_version,
        "connection_state": connection_state,
        "account_type": account_type,
        "duration_secs": duration_secs,
        "total_events": total,
        "farm_state_counts": farm_counts,
        "events": events_json,
    });

    println!("{}", serde_json::to_string_pretty(&report).unwrap());
}
