use crate::{
    config::ServerConfig,
    types::id::{JournalId, PageIdentifier},
};

pub trait BlobConfig: Send + Sync + Clone {
    fn bucket_name(&self, logid: &JournalId) -> String;
    fn page_name(&self, logid: &JournalId, page: &PageIdentifier) -> String;
    fn region(&self) -> &str;
    fn isolate(&self) -> bool {
        false
    }
    fn workers(&self) -> usize; 
    fn has_constant_bucket(&self) -> bool {
        false
    }
}

impl BlobConfig for &'static ServerConfig {
    fn bucket_name(&self, id: &JournalId) -> String {
        replace_vars(
            &self.bucket_name_pattern.as_str(),
            &[("logid", id.as_hyphenated().to_string().as_str()), ("region", self.region())],
        )
    }

    fn has_constant_bucket(&self) -> bool {
        !self.bucket_name_pattern.contains("{logid}")
    }

    fn page_name(&self, id: &JournalId, page: &PageIdentifier) -> String {
        replace_vars(
            &self.object_prefix_pattern.as_str(),
            &[
                ("logid", id.as_hyphenated().to_string().as_str()),
                ("epoch", page.0.to_string().as_str()),
                ("max_tid", page.1.to_string().as_str()),
                ("region", self.region()),
            ],
        )
    }

    fn region(&self) -> &str {
        &self.aws_region
    }

    fn isolate(&self) -> bool {
        matches!(self.blob_store_impl, crate::config::BlobStoreImplementation::S3)
    }

    fn workers(&self) -> usize {
        self.blob_store_workers
    }
}

fn replace_vars(pattern: &str, replacements: &[(&str, &str)]) -> String {
    let mut result = pattern.to_string();
    for (var, value) in replacements {
        let var_pattern = format!("{{{}}}", var);
        result = result.replace(&var_pattern, value);
    }
    result
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;

    #[derive(Clone)]
    pub struct MockConfig;

    impl BlobConfig for MockConfig {
        fn bucket_name(&self, _logid: &JournalId) -> String {
            "journal-service-test-bucket".to_owned()
        }

        fn page_name(&self, logid: &JournalId, page: &PageIdentifier) -> String {
            replace_vars(
                "test-log-{logid}/page-{epoch}-{lsn}",
                &[
                    ("logid", logid.as_hyphenated().to_string().as_str()),
                    ("epoch", page.0.to_string().as_str()),
                    ("lsn", page.1.to_string().as_str()),
                ],
            )
        }

        fn region(&self) -> &str {
            "eu-central-1"
        }

        fn workers(&self) -> usize {
            1
        }
    }

    #[test]
    fn test_replace_vars() {
        let pattern = "Hello, {name}! Welcome to {place}.";
        let replacements = [("name", "Alice"), ("place", "Wonderland")];
        let result = replace_vars(pattern, &replacements);
        assert_eq!(result, "Hello, Alice! Welcome to Wonderland.");
    }

    #[test]
    fn test_blob_config() {
        let config = MockConfig {};
        let uuid = JournalId::nil();
        assert_eq!(
            config.page_name(&uuid, &PageIdentifier(1, 2, 0)),
            "test-log-00000000-0000-0000-0000-000000000000/page-1-2"
        );
        assert_eq!(config.region(), "eu-central-1");
    }
}
