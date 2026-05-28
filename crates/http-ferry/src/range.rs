use url::Url;

use crate::Error;

pub(crate) fn validate_content_range(
    response: &reqwest::Response,
    expected_start: u64,
    expected_total: u64,
    url: &Url,
) -> Result<(), Error> {
    let header = response
        .headers()
        .get(reqwest::header::CONTENT_RANGE)
        .ok_or_else(|| Error::InvalidRangeResponse {
            url: url.to_string(),
            details: "missing Content-Range header on 206 response".into(),
        })?;
    let value = header.to_str().map_err(|_| Error::InvalidRangeResponse {
        url: url.to_string(),
        details: "invalid Content-Range header encoding".into(),
    })?;
    validate_content_range_value(value, expected_start, expected_total, url)
}

fn validate_content_range_value(
    value: &str,
    expected_start: u64,
    expected_total: u64,
    url: &Url,
) -> Result<(), Error> {
    let range = value
        .strip_prefix("bytes ")
        .ok_or_else(|| Error::InvalidRangeResponse {
            url: url.to_string(),
            details: format!("unexpected Content-Range format: {value}"),
        })?;
    let (bounds, total) = range
        .split_once('/')
        .ok_or_else(|| Error::InvalidRangeResponse {
            url: url.to_string(),
            details: format!("unexpected Content-Range format: {value}"),
        })?;
    let (start, _) = bounds
        .split_once('-')
        .ok_or_else(|| Error::InvalidRangeResponse {
            url: url.to_string(),
            details: format!("unexpected Content-Range format: {value}"),
        })?;
    let start = start
        .parse::<u64>()
        .map_err(|_| Error::InvalidRangeResponse {
            url: url.to_string(),
            details: format!("invalid Content-Range start: {value}"),
        })?;
    let total = total
        .parse::<u64>()
        .map_err(|_| Error::InvalidRangeResponse {
            url: url.to_string(),
            details: format!("invalid Content-Range total: {value}"),
        })?;

    if start != expected_start {
        return Err(Error::InvalidRangeResponse {
            url: url.to_string(),
            details: format!("Content-Range starts at {start}, expected {expected_start}"),
        });
    }
    if total != expected_total {
        return Err(Error::InvalidRangeResponse {
            url: url.to_string(),
            details: format!("Content-Range total is {total}, expected {expected_total}"),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn url() -> Url {
        Url::parse("https://example.invalid/foo").unwrap()
    }

    fn check(value: &str) -> Result<(), Error> {
        validate_content_range_value(value, 100, 1000, &url())
    }

    #[test]
    fn accepts_well_formed_range() {
        check("bytes 100-999/1000").unwrap();
    }

    #[test]
    fn rejects_value_without_bytes_prefix() {
        let err = check("100-999/1000").unwrap_err();
        assert!(matches!(err, Error::InvalidRangeResponse { details, .. }
            if details.contains("unexpected Content-Range format")));
    }

    #[test]
    fn rejects_value_with_no_total_separator() {
        let err = check("bytes 100-999").unwrap_err();
        assert!(matches!(err, Error::InvalidRangeResponse { details, .. }
            if details.contains("unexpected Content-Range format")));
    }

    #[test]
    fn rejects_bounds_with_no_dash() {
        let err = check("bytes 100/1000").unwrap_err();
        assert!(matches!(err, Error::InvalidRangeResponse { details, .. }
            if details.contains("unexpected Content-Range format")));
    }

    #[test]
    fn rejects_non_numeric_start() {
        let err = check("bytes abc-999/1000").unwrap_err();
        assert!(matches!(err, Error::InvalidRangeResponse { details, .. }
            if details.contains("invalid Content-Range start")));
    }

    #[test]
    fn rejects_non_numeric_total() {
        let err = check("bytes 100-999/xyz").unwrap_err();
        assert!(matches!(err, Error::InvalidRangeResponse { details, .. }
            if details.contains("invalid Content-Range total")));
    }

    #[test]
    fn rejects_start_that_does_not_match_expected() {
        let err = check("bytes 50-999/1000").unwrap_err();
        assert!(matches!(err, Error::InvalidRangeResponse { details, .. }
            if details.contains("starts at 50")));
    }

    #[test]
    fn rejects_total_that_does_not_match_expected() {
        let err = check("bytes 100-999/2000").unwrap_err();
        assert!(matches!(err, Error::InvalidRangeResponse { details, .. }
            if details.contains("total is 2000")));
    }
}
