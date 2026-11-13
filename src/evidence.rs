use crate::config::EvidenceConfig;
use anyhow::{anyhow, Result};
use futures_util::StreamExt;
use hmac::{Hmac, Mac};
use reqwest::Client;
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;

type HmacSha256 = Hmac<Sha256>;

fn object_url(config: &EvidenceConfig, bucket: &str, key: &str) -> Result<String> {
    let endpoint = config.endpoint.trim_end_matches('/');
    if config.path_style {
        Ok(format!("{}/{}/{}", endpoint, bucket, encode_path(key)))
    } else {
        let scheme_end = endpoint.find("://").map(|idx| idx + 3).unwrap_or(0);
        let (scheme, rest) = endpoint.split_at(scheme_end);
        Ok(format!(
            "{}{}.{}/{}",
            scheme,
            bucket,
            rest.trim_end_matches('/'),
            encode_path(key)
        ))
    }
}

pub async fn upload_evidence(
    config: &EvidenceConfig,
    key: &str,
    body: Vec<u8>,
    content_type: &str,
) -> Result<()> {
    if config.endpoint.is_empty() || config.bucket.is_empty() {
        return Err(anyhow!("Evidence S3 is not configured"));
    }

    let client = Client::new();
    let url = object_url(config, &config.bucket, key)?;

    let now = chrono::Utc::now();
    let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();
    let date_stamp = now.format("%Y%m%d").to_string();
    let payload_hash = hex::encode(Sha256::digest(&body));
    let parsed = reqwest::Url::parse(&url)?;
    let host = parsed
        .host_str()
        .ok_or_else(|| anyhow!("Invalid S3 endpoint"))?
        .to_string();
    let host = if let Some(port) = parsed.port() {
        format!("{}:{}", host, port)
    } else {
        host
    };

    let canonical_headers = format!(
        "content-type:{}\nhost:{}\nx-amz-content-sha256:{}\nx-amz-date:{}\n",
        content_type, host, payload_hash, amz_date
    );
    let signed_headers = "content-type;host;x-amz-content-sha256;x-amz-date";
    let canonical_request = format!(
        "PUT\n{}\n\n{}\n{}\n{}",
        parsed.path(),
        canonical_headers,
        signed_headers,
        payload_hash
    );
    let scope = format!("{}/{}/s3/aws4_request", date_stamp, config.region);
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{}\n{}\n{}",
        amz_date,
        scope,
        hex::encode(Sha256::digest(canonical_request.as_bytes()))
    );

    let k_date = hmac(
        format!("AWS4{}", config.secret_key).as_bytes(),
        date_stamp.as_bytes(),
    )?;
    let k_region = hmac(&k_date, config.region.as_bytes())?;
    let k_service = hmac(&k_region, b"s3")?;
    let k_signing = hmac(&k_service, b"aws4_request")?;
    let signature = hex::encode(hmac(&k_signing, string_to_sign.as_bytes())?);

    let authorization = format!(
        "AWS4-HMAC-SHA256 Credential={}/{}, SignedHeaders={}, Signature={}",
        config.access_key, scope, signed_headers, signature
    );

    let response = client
        .put(url)
        .header("content-type", content_type)
        .header("x-amz-content-sha256", payload_hash)
        .header("x-amz-date", amz_date)
        .header("authorization", authorization)
        .body(body)
        .send()
        .await?;

    if !response.status().is_success() {
        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        return Err(anyhow!("Evidence upload failed ({}): {}", status, text));
    }
    Ok(())
}

pub async fn download_object(
    client: &Client,
    config: &EvidenceConfig,
    bucket: &str,
    key: &str,
    path: &str,
    max_bytes: u64,
) -> Result<()> {
    if config.endpoint.is_empty() || bucket.is_empty() {
        return Err(anyhow!("Catalog S3 is not configured"));
    }

    let url = object_url(config, bucket, key)?;
    let now = chrono::Utc::now();
    let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();
    let date_stamp = now.format("%Y%m%d").to_string();
    let payload_hash = hex::encode(Sha256::digest(b""));
    let parsed = reqwest::Url::parse(&url)?;
    let host = parsed
        .host_str()
        .ok_or_else(|| anyhow!("Invalid S3 endpoint"))?
        .to_string();
    let host = if let Some(port) = parsed.port() {
        format!("{}:{}", host, port)
    } else {
        host
    };

    let canonical_headers = format!(
        "host:{}\nx-amz-content-sha256:{}\nx-amz-date:{}\n",
        host, payload_hash, amz_date
    );
    let signed_headers = "host;x-amz-content-sha256;x-amz-date";
    let canonical_request = format!(
        "GET\n{}\n\n{}\n{}\n{}",
        parsed.path(),
        canonical_headers,
        signed_headers,
        payload_hash
    );
    let scope = format!("{}/{}/s3/aws4_request", date_stamp, config.region);
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{}\n{}\n{}",
        amz_date,
        scope,
        hex::encode(Sha256::digest(canonical_request.as_bytes()))
    );

    let k_date = hmac(
        format!("AWS4{}", config.secret_key).as_bytes(),
        date_stamp.as_bytes(),
    )?;
    let k_region = hmac(&k_date, config.region.as_bytes())?;
    let k_service = hmac(&k_region, b"s3")?;
    let k_signing = hmac(&k_service, b"aws4_request")?;
    let signature = hex::encode(hmac(&k_signing, string_to_sign.as_bytes())?);

    let authorization = format!(
        "AWS4-HMAC-SHA256 Credential={}/{}, SignedHeaders={}, Signature={}",
        config.access_key, scope, signed_headers, signature
    );

    let response = client
        .get(url)
        .header("x-amz-content-sha256", payload_hash)
        .header("x-amz-date", amz_date)
        .header("authorization", authorization)
        .send()
        .await?;

    if !response.status().is_success() {
        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        return Err(anyhow!(
            "Catalog object download failed ({}): {}",
            status,
            text
        ));
    }

    if let Some(length) = response.content_length() {
        if length > max_bytes {
            return Err(anyhow!("Catalog object exceeds {} bytes", max_bytes));
        }
    }

    let mut file = tokio::fs::File::create(path).await?;
    let mut stream = response.bytes_stream();
    let mut total = 0_u64;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        total += chunk.len() as u64;
        if total > max_bytes {
            drop(file);
            let _ = tokio::fs::remove_file(path).await;
            return Err(anyhow!("Catalog object exceeds {} bytes", max_bytes));
        }
        file.write_all(&chunk).await?;
    }
    file.flush().await?;
    Ok(())
}

fn hmac(key: &[u8], data: &[u8]) -> Result<Vec<u8>> {
    let mut mac = HmacSha256::new_from_slice(key)?;
    mac.update(data);
    Ok(mac.finalize().into_bytes().to_vec())
}

fn encode_path(key: &str) -> String {
    key.split('/')
        .map(|segment| {
            let mut out = String::new();
            for byte in segment.bytes() {
                match byte {
                    b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                        out.push(byte as char)
                    }
                    _ => out.push_str(&format!("%{:02X}", byte)),
                }
            }
            out
        })
        .collect::<Vec<_>>()
        .join("/")
}
