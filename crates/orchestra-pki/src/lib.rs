//! ORCHESTRA cluster PKI.
//!
//! One self-signed certificate authority per cluster issues every credential:
//! the control plane's server certificate and the client certificates that
//! identify administrators and node agents. Identity lives in the certificate
//! subject: the organization is the role, the common name is the principal.
//!
//! Certificates are never revoked; node certificates are short-lived and
//! rotated instead.

#[cfg(feature = "client")]
pub mod client;

use std::time::Duration;

use rcgen::{
    BasicConstraints, CertificateParams, CertificateSigningRequestParams, DistinguishedName, DnType,
    ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair, KeyUsagePurpose, PublicKeyData,
};
use time::OffsetDateTime;

/// Subject organization of administrator certificates.
pub const ORG_ADMINS: &str = "orchestra:admins";
/// Subject organization of node agent certificates.
pub const ORG_NODES: &str = "orchestra:nodes";

const CA_VALIDITY: Duration = Duration::from_secs(10 * 365 * 24 * 3600);
/// Tolerated clock difference between the issuer and whoever verifies.
const CLOCK_SKEW: Duration = Duration::from_secs(60);

#[derive(Debug, thiserror::Error)]
pub enum PkiError {
    #[error("certificate generation failed: {0}")]
    Generation(#[from] rcgen::Error),
    #[error("invalid certificate signing request: {0}")]
    InvalidCsr(rcgen::Error),
    #[error("invalid certificate: {0}")]
    InvalidCertificate(String),
    #[error("certificate does not carry an ORCHESTRA identity")]
    UnknownIdentity,
}

pub type Result<T> = std::result::Result<T, PkiError>;

/// Who a client certificate speaks for.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Identity {
    Admin(String),
    Node(String),
}

impl Identity {
    fn subject(&self) -> (&str, &'static str) {
        match self {
            Identity::Admin(name) => (name, ORG_ADMINS),
            Identity::Node(name) => (name, ORG_NODES),
        }
    }
}

/// A certificate and its private key, PEM-encoded.
#[derive(Clone)]
pub struct KeyMaterial {
    pub cert_pem: String,
    pub key_pem: String,
}

/// Writes a file readable by its owner only, replacing any previous version
/// atomically so that a crash never leaves a half-written credential behind.
pub fn write_private_file(path: &std::path::Path, content: &str) -> std::io::Result<()> {
    use std::{io::Write, os::unix::fs::OpenOptionsExt};

    let tmp = path.with_extension("tmp");
    let _ = std::fs::remove_file(&tmp);
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&tmp)?;
    file.write_all(content.as_bytes())?;
    file.sync_all()?;
    std::fs::rename(&tmp, path)
}

impl KeyMaterial {
    /// Certificate followed by private key, in one PEM bundle.
    pub fn to_bundle(&self) -> String {
        format!("{}\n{}", self.cert_pem.trim_end(), self.key_pem)
    }

    /// Splits a bundle produced by [`KeyMaterial::to_bundle`].
    pub fn from_bundle(bundle: &str) -> Result<KeyMaterial> {
        const END: &str = "-----END CERTIFICATE-----";
        let split = bundle
            .find(END)
            .map(|index| index + END.len())
            .ok_or_else(|| PkiError::InvalidCertificate("bundle contains no certificate".into()))?;
        let (cert, key) = bundle.split_at(split);
        if !key.contains("PRIVATE KEY") {
            return Err(PkiError::InvalidCertificate(
                "bundle contains no private key".into(),
            ));
        }
        Ok(KeyMaterial {
            cert_pem: format!("{cert}\n"),
            key_pem: key.trim_start().to_string(),
        })
    }
}

/// Installs the process-wide TLS crypto provider. Call once at startup,
/// before any TLS configuration is built.
pub fn install_crypto_provider() {
    // Fails only if a provider is already installed, which is fine.
    let _ = rustls::crypto::ring::default_provider().install_default();
}

pub struct CertificateAuthority {
    cert_pem: String,
    key_pem: String,
    issuer: Issuer<'static, KeyPair>,
}

impl CertificateAuthority {
    /// Creates a new self-signed authority that can only sign leaf certificates.
    pub fn generate(cluster_name: &str) -> Result<Self> {
        let key = KeyPair::generate()?;
        let mut params = CertificateParams::default();
        params.distinguished_name = DistinguishedName::new();
        params
            .distinguished_name
            .push(DnType::CommonName, format!("{cluster_name} CA"));
        params
            .distinguished_name
            .push(DnType::OrganizationName, "orchestra");
        params.is_ca = IsCa::Ca(BasicConstraints::Constrained(0));
        params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        set_validity(&mut params, CA_VALIDITY);
        let cert_pem = params.self_signed(&key)?.pem();
        Self::from_pem(&cert_pem, &key.serialize_pem())
    }

    pub fn from_pem(cert_pem: &str, key_pem: &str) -> Result<Self> {
        let key = KeyPair::from_pem(key_pem)?;
        let issuer = Issuer::from_ca_cert_pem(cert_pem, key)?;
        Ok(CertificateAuthority {
            cert_pem: cert_pem.to_string(),
            key_pem: key_pem.to_string(),
            issuer,
        })
    }

    pub fn cert_pem(&self) -> &str {
        &self.cert_pem
    }

    pub fn key_pem(&self) -> &str {
        &self.key_pem
    }

    /// Issues a TLS server certificate valid for `names` (DNS names or IP addresses).
    pub fn issue_server(&self, names: &[String], ttl: Duration) -> Result<KeyMaterial> {
        let key = KeyPair::generate()?;
        let mut params = leaf_params(ttl, ExtendedKeyUsagePurpose::ServerAuth);
        params.subject_alt_names = CertificateParams::new(names.to_vec())?.subject_alt_names;
        params
            .distinguished_name
            .push(DnType::CommonName, "orchestra-server");
        self.sign(&params, &key, &key)
    }

    /// Issues a client certificate together with a fresh private key.
    pub fn issue_client(&self, identity: &Identity, ttl: Duration) -> Result<KeyMaterial> {
        let key = KeyPair::generate()?;
        self.sign(&client_params(identity, ttl), &key, &key)
    }

    /// Signs a client certificate for the public key of a signing request.
    ///
    /// Only the key is taken from the request (after checking the request's
    /// signature, which proves possession of the private key). The subject is
    /// imposed by the caller: a requester never chooses its own identity.
    pub fn sign_client_csr(&self, csr_pem: &str, identity: &Identity, ttl: Duration) -> Result<String> {
        let request = CertificateSigningRequestParams::from_pem(csr_pem).map_err(PkiError::InvalidCsr)?;
        let certificate = client_params(identity, ttl).signed_by(&request.public_key, &self.issuer)?;
        Ok(certificate.pem())
    }

    fn sign(
        &self,
        params: &CertificateParams,
        public_key: &impl PublicKeyData,
        key: &KeyPair,
    ) -> Result<KeyMaterial> {
        let certificate = params.signed_by(public_key, &self.issuer)?;
        Ok(KeyMaterial {
            cert_pem: certificate.pem(),
            key_pem: key.serialize_pem(),
        })
    }
}

fn set_validity(params: &mut CertificateParams, ttl: Duration) {
    let now = OffsetDateTime::now_utc();
    params.not_before = now - CLOCK_SKEW;
    params.not_after = now + ttl;
}

fn leaf_params(ttl: Duration, usage: ExtendedKeyUsagePurpose) -> CertificateParams {
    let mut params = CertificateParams::default();
    params.distinguished_name = DistinguishedName::new();
    params.is_ca = IsCa::ExplicitNoCa;
    params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
    params.extended_key_usages = vec![usage];
    params.use_authority_key_identifier_extension = true;
    set_validity(&mut params, ttl);
    params
}

fn client_params(identity: &Identity, ttl: Duration) -> CertificateParams {
    let mut params = leaf_params(ttl, ExtendedKeyUsagePurpose::ClientAuth);
    let (name, organization) = identity.subject();
    params.distinguished_name.push(DnType::CommonName, name);
    params
        .distinguished_name
        .push(DnType::OrganizationName, organization);
    params
}

/// Generates a private key and a signing request for it. The subject is left
/// empty on purpose: the authority decides it.
pub fn generate_csr() -> Result<(String, String)> {
    let key = KeyPair::generate()?;
    let mut params = CertificateParams::default();
    params.distinguished_name = DistinguishedName::new();
    let csr = params.serialize_request(&key)?.pem()?;
    Ok((csr, key.serialize_pem()))
}

/// Reads the identity out of a certificate that has **already been verified**
/// against the cluster CA (by the TLS handshake). This function does not
/// check signatures or validity itself.
pub fn identity_from_der(der: &[u8]) -> Result<Identity> {
    let (_, certificate) =
        x509_parser::parse_x509_certificate(der).map_err(|e| PkiError::InvalidCertificate(e.to_string()))?;
    let subject = certificate.subject();
    let text = |attribute: Option<&x509_parser::x509::AttributeTypeAndValue<'_>>| {
        attribute.and_then(|a| a.as_str().ok()).map(str::to_string)
    };
    let name = text(subject.iter_common_name().next()).ok_or(PkiError::UnknownIdentity)?;
    match text(subject.iter_organization().next()).as_deref() {
        Some(ORG_ADMINS) => Ok(Identity::Admin(name)),
        Some(ORG_NODES) => Ok(Identity::Node(name)),
        _ => Err(PkiError::UnknownIdentity),
    }
}

/// Validity window of a PEM certificate, as Unix timestamps in seconds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Validity {
    pub not_before: i64,
    pub not_after: i64,
}

impl Validity {
    pub fn is_valid_at(&self, unix_secs: i64) -> bool {
        (self.not_before..=self.not_after).contains(&unix_secs)
    }

    /// True once less than a third of the lifetime remains (or it has expired).
    pub fn should_renew_at(&self, unix_secs: i64) -> bool {
        let lifetime = self.not_after - self.not_before;
        self.not_after - unix_secs < lifetime / 3
    }
}

pub fn validity(cert_pem: &str) -> Result<Validity> {
    let invalid = |e: String| PkiError::InvalidCertificate(e);
    let (_, pem) =
        x509_parser::pem::parse_x509_pem(cert_pem.as_bytes()).map_err(|e| invalid(e.to_string()))?;
    let certificate = pem.parse_x509().map_err(|e| invalid(e.to_string()))?;
    let validity = certificate.validity();
    Ok(Validity {
        not_before: validity.not_before.timestamp(),
        not_after: validity.not_after.timestamp(),
    })
}

#[cfg(test)]
mod tests {
    use rustls::pki_types::{CertificateDer, UnixTime, pem::PemObject};
    use rustls::server::WebPkiClientVerifier;

    use super::*;

    const DAY: Duration = Duration::from_secs(24 * 3600);

    fn der(pem: &str) -> CertificateDer<'static> {
        CertificateDer::from_pem_slice(pem.as_bytes()).unwrap()
    }

    /// Verifies a client certificate exactly as the server's handshake does.
    fn verify_client(ca: &CertificateAuthority, cert_pem: &str) -> std::result::Result<(), rustls::Error> {
        install_crypto_provider();
        let mut roots = rustls::RootCertStore::empty();
        roots.add(der(ca.cert_pem())).unwrap();
        let verifier = WebPkiClientVerifier::builder(roots.into()).build().unwrap();
        verifier
            .verify_client_cert(&der(cert_pem), &[], UnixTime::now())
            .map(|_| ())
    }

    #[test]
    fn issued_identities_round_trip_and_verify() {
        let ca = CertificateAuthority::generate("test").unwrap();
        for identity in [Identity::Admin("alice".into()), Identity::Node("node-a".into())] {
            let material = ca.issue_client(&identity, DAY).unwrap();
            verify_client(&ca, &material.cert_pem).unwrap();
            assert_eq!(identity_from_der(&der(&material.cert_pem)).unwrap(), identity);
        }
    }

    #[test]
    fn ca_reloads_from_pem() {
        let ca = CertificateAuthority::generate("test").unwrap();
        let reloaded = CertificateAuthority::from_pem(ca.cert_pem(), ca.key_pem()).unwrap();
        let material = reloaded.issue_client(&Identity::Node("n".into()), DAY).unwrap();
        verify_client(&ca, &material.cert_pem).unwrap();
    }

    #[test]
    fn certificates_from_another_ca_are_rejected() {
        let ours = CertificateAuthority::generate("ours").unwrap();
        let theirs = CertificateAuthority::generate("theirs").unwrap();
        let forged = theirs
            .issue_client(&Identity::Admin("mallory".into()), DAY)
            .unwrap();
        assert!(verify_client(&ours, &forged.cert_pem).is_err());
    }

    #[test]
    fn server_certificates_cannot_authenticate_clients() {
        let ca = CertificateAuthority::generate("test").unwrap();
        let server = ca
            .issue_server(&["localhost".into(), "127.0.0.1".into()], DAY)
            .unwrap();
        assert!(verify_client(&ca, &server.cert_pem).is_err());
        assert!(matches!(
            identity_from_der(&der(&server.cert_pem)),
            Err(PkiError::UnknownIdentity)
        ));
    }

    #[test]
    fn csr_subject_is_ignored_and_bad_requests_rejected() {
        let ca = CertificateAuthority::generate("test").unwrap();
        // A requester asking to be an administrator...
        let key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::default();
        params.distinguished_name = DistinguishedName::new();
        params.distinguished_name.push(DnType::CommonName, "root");
        params
            .distinguished_name
            .push(DnType::OrganizationName, ORG_ADMINS);
        let csr = params.serialize_request(&key).unwrap().pem().unwrap();
        // ...only ever gets the identity the authority decided on.
        let cert = ca
            .sign_client_csr(&csr, &Identity::Node("node-a".into()), DAY)
            .unwrap();
        verify_client(&ca, &cert).unwrap();
        assert_eq!(
            identity_from_der(&der(&cert)).unwrap(),
            Identity::Node("node-a".into())
        );

        let garbage = ca.sign_client_csr("not a csr", &Identity::Node("n".into()), DAY);
        assert!(matches!(garbage, Err(PkiError::InvalidCsr(_))));
    }

    #[test]
    fn bundles_round_trip_through_private_files() {
        let ca = CertificateAuthority::generate("test").unwrap();
        let material = ca.issue_client(&Identity::Node("n".into()), DAY).unwrap();
        let dir = std::env::temp_dir().join(format!("orchestra-pki-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("node.pem");
        write_private_file(&path, &material.to_bundle()).unwrap();
        write_private_file(&path, &material.to_bundle()).unwrap();

        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        let loaded = KeyMaterial::from_bundle(&std::fs::read_to_string(&path).unwrap()).unwrap();
        verify_client(&ca, &loaded.cert_pem).unwrap();
        assert_eq!(loaded.key_pem, material.key_pem);
        assert!(KeyMaterial::from_bundle(&material.cert_pem).is_err());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn validity_and_renewal_window() {
        let ca = CertificateAuthority::generate("test").unwrap();
        let (csr, _key) = generate_csr().unwrap();
        let cert = ca
            .sign_client_csr(&csr, &Identity::Node("n".into()), Duration::from_secs(3000))
            .unwrap();
        let window = validity(&cert).unwrap();
        let now = OffsetDateTime::now_utc().unix_timestamp();
        assert!(window.is_valid_at(now));
        assert!(!window.is_valid_at(window.not_after + 1));
        assert!(!window.should_renew_at(now));
        assert!(window.should_renew_at(window.not_after - 900));
        assert!(window.should_renew_at(window.not_after + 10));
    }
}
