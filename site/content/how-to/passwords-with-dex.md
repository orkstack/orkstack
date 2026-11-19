# Sign in with a password

Let people sign in with a username and password when you have no identity provider. OrkStack keeps no passwords of its own: you run Dex, a small OpenID Connect provider, and OrkStack trusts it.

## Run Dex with your users

Use the Dex image OrkStack is tested with:

```text
ghcr.io/dexidp/dex:v2.45.1@sha256:8499afd690c437f52301efd2b05b2455da5bd2dfc20332cd697dc9937f808462
```

Give it a configuration like this:

```yaml
issuer: https://dex.example.com/dex
storage:
  type: sqlite3
  config:
    file: /var/dex/dex.db
web:
  https: 0.0.0.0:5554
  tlsCert: /etc/dex/tls/tls.crt
  tlsKey: /etc/dex/tls/tls.key
expiry:
  idTokens: 1h
oauth2:
  responseTypes:
    - code
  skipApprovalScreen: true
enablePasswordDB: true
staticPasswords:
  - email: alice@example.com
    # bcrypt of the example password `example-only`: replace it.
    hash: $2a$10$ooVYj3p6Jn2AMW.w6vwwwe3KSKNo48Off2uiMlIFsYhJoRl00EZ.y
    username: alice
    userID: 0b9d8f4e-3c1a-4f8e-9a52-6d1f2e7c4a10
    emailVerified: true
    groups:
      - platform-admins
staticClients:
  - id: orkstack-web
    name: OrkStack
    secretEnv: ORKSTACK_WEB_CLIENT_SECRET
    redirectURIs:
      - https://orkstack.example.com/v1/auth/callback/people
  - id: orkctl
    name: orkctl
    public: true
```

Replace the example hash: its password is published here, and Alice is an admin. Store a bcrypt hash of each password, never the password itself:

```bash
htpasswd -nbBC 10 '' 'THE-PASSWORD' | tr -d ':\n'
```

Each user needs `emailVerified: true`, because OrkStack names people by their email. Adding a user or changing a password means editing the file and restarting Dex.

Dex listens on port 5554. Publish it at the issuer URL, for example on 443 behind a load balancer, or put the port in both issuer URLs.

## Make a client secret

```bash
(umask 077; head -c 32 /dev/urandom | base64 | tr -d '=+/' > /etc/orkstack/web-client.secret)
```

Give the same value to Dex as `ORKSTACK_WEB_CLIENT_SECRET`.

## Trust Dex in OrkStack

```yaml
auth:
  mode: enforce
  public_url: https://orkstack.example.com
  admins:
    - group:people:platform-admins
  signin:
    key_files:
      - /etc/orkstack/signin.key
  issuers:
    people:
      issuer: https://dex.example.com/dex
      kind: user
      audiences:
        - orkstack-web
        - orkctl
      algorithms:
        - RS256
      username_claim: email
      require_verified_email: true
      groups_claim: groups
      max_lifetime: 2h
      login:
        client_id: orkstack-web
        client_secret:
          source: file:/etc/orkstack/web-client.secret
        cli_client_id: orkctl
        device_flow: true
        scopes:
          - openid
          - email
          - groups
          - offline_access
identity:
  signing:
    key_files:
      - /etc/orkstack/engine-signing.pem
```

Alice now signs in to the web UI with her email and password, and is an admin through her group. In a terminal she runs `orkctl login`, or `orkctl login --device` where no browser opens.

## Give other people roles

```bash
orkctl admin binding create ops --role operator --scope namespace:payments --subject group:people:payments-ops
```

## Before you go to production

- Serve Dex over HTTPS, at an address that does not change.
- Keep Dex's storage on a persistent volume. With `memory` storage every restart signs everyone out.
- Put a rate limit in front of Dex's login page. Its password database has no lockout, password reset or second factor.
- To remove someone, first run `orkctl admin principal disable user:people:alice@example.com`, which cuts them off at once, then delete their entry and restart Dex.
- Dex rotates its own signing keys; OrkStack picks up the new keys by itself.

Later, the same Dex can sign people in through LDAP, GitHub or another provider. Add a connector to Dex; OrkStack's configuration stays the same.

## See also

- [AUTHENTICATION.md, "Passwords, through Dex"](https://github.com/orkstack/orkstack/blob/main/docs/AUTHENTICATION.md#passwords-through-dex): every setting explained, and production hardening
- [AUTHENTICATION.md, "Sign-in for the web UI"](https://github.com/orkstack/orkstack/blob/main/docs/AUTHENTICATION.md#sign-in-for-the-web-ui): how sign-in works, and the sign-in key
- [ACCESS.md, "Built-in roles"](https://github.com/orkstack/orkstack/blob/main/docs/ACCESS.md#built-in-roles): what each role may do
