use serde::Deserialize;
use serde_json::Value;

use super::Metadata;

#[derive(Debug, Clone, Deserialize)]
pub struct Account {
    pub id: u64,
    pub organization_name: String,
    pub partner_description: Option<String>,
    pub logo_blob: Option<u64>,
    pub partner_url: String,
    pub custom_crawl_schedules_visible: bool,

    pub created_by: String,
    pub created_date: String,
    pub last_updated_by: String,
    pub last_updated_date: String,
    pub member_since_date: String,
    pub billing_period_start_date: Option<String>,
    pub subscription_end_date: Option<String>,

    pub account_type: String,
    pub partner_type: String,
    pub brozzler_option_visible: String,

    pub active: bool,
    pub deleted: bool,
    pub hidden: bool,
    pub feed_enabled: bool,
    pub tos_enabled: bool,
    pub metadata_public: bool,
    pub public_registry_enabled: bool,
    pub ignore_robots_option_visible: bool,
    pub ignore_delay_option_visible: bool,
    pub warc_upload_link_visible: bool,
    pub show_longer_crawl_durations: bool,
    pub enforce_budget: Option<bool>,

    pub invoice_data_budget_in_gbs: u64,
    pub ledger_data_budget_in_gbs: Option<u64>,
    pub default_crawl_limit_in_gbs: Option<u64>,
    pub max_concurrent_test_crawls: u64,
    pub annual_subscription_cost: i64,
    pub subscription_currency: String,

    pub google_analytics_id: Option<String>,
    pub oclc_api_key: String,
    pub custom_name: Option<String>,
    pub custom_user_agent: Option<String>,

    pub billing_name: Option<String>,
    pub billing_email: Option<String>,
    pub billing_address: Option<String>,

    pub auto_renew: Option<bool>,

    pub public_site_settings: Option<Value>,
    pub private_metadata_fields: Option<Value>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Collection {
    pub id: u64,
    pub account: u64,
    pub name: String,
    pub state: String,
    pub publicly_visible: bool,
    pub deleted: bool,
    pub oai_exported: bool,
    pub topics: Option<String>,
    pub image: Option<u64>,
    pub created_by: String,
    pub created_date: String,
    pub last_updated_by: String,
    pub last_updated_date: String,
    pub last_crawl_date: Option<String>,
    pub num_active_seeds: u64,
    pub num_inactive_seeds: u64,
    pub total_warc_bytes: u64,
    pub custom_user_agent: Option<String>,
    pub private_access_token: String,
    pub metadata: Option<Metadata>,
}
