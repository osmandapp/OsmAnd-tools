#!/usr/bin/env python3
"""Log in to seabed.admiralty.co.uk (Azure AD B2C) and print fresh tokens as shell exports.

Usage: eval "$(login_ukho.py)"   # sets UKHO_TOKEN and UKHO_REFRESH_TOKEN

The B2C flow: authorize (PKCE) -> SelfAsserted email step -> CombinedSigninAndSignup
password step -> code redemption, over plain curl (the API blocks urllib's TLS fingerprint).
Credentials from UKHO_EMAIL/UKHO_PASSWORD env or argv. Never hardcode them here.
"""
import base64, hashlib, json, re, secrets, subprocess, sys, tempfile, os, urllib.parse

AUTH = "https://mgiaidprdb2c.b2clogin.com/mgiaidprdb2c.onmicrosoft.com/B2C_1A_SignIn_SPA"
TOKEN_URL = AUTH + "/oauth2/v2.0/token"
CLIENT_ID = "5aeea2b8-3a8f-44bc-9b1c-c6b39d4a9f87"  # the public client of seabed.admiralty.co.uk
REDIRECT = "https://seabed.admiralty.co.uk/"
SCOPE = ("openid offline_access "
         "https://mgiaidprdb2c.onmicrosoft.com/b074f352-4580-47b2-b9a5-e3c1f160c4db/access")
UA = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
      "Chrome/126.0 Safari/537.36")


def curl(jar, out, args, fail=True):
    r = subprocess.run(["curl", "-s", "-4", "-c", jar, "-b", jar, "-D", out + ".hdr", "-o", out,
                        "-A", UA, "--max-time", "120"] + args, capture_output=True, text=True)
    if r.returncode != 0:
        raise SystemExit(f"curl failed ({r.returncode}): {r.stderr.strip()[:200]}")
    hdrs = open(out + ".hdr", errors="replace").read()
    return hdrs, open(out, errors="replace").read()


def settings_of(html, where):
    m = re.search(r"var SETTINGS = (\{.*?\});", html, re.S)
    if not m:
        raise SystemExit(f"no SETTINGS at {where} ({len(html)} bytes)")
    return json.loads(m.group(1))


def post_self_asserted(jar, tx, csrf, **fields):
    body = urllib.parse.urlencode(fields)
    # the SelfAsserted endpoint double-decodes tx: it must be sent double-encoded (server quirk)
    tx2 = urllib.parse.quote(urllib.parse.quote(tx, safe=""), safe="")
    resp = subprocess.run(["curl", "-s", "-4", "-c", jar, "-b", jar,
                           "-X", "POST", f"{AUTH}/SelfAsserted?tx={tx2}&p=B2C_1A_SignIn_SPA",
                           "-H", f"X-CSRF-TOKEN: {csrf}", "-H", "X-Requested-With: XMLHttpRequest",
                           "-H", "Content-Type: application/x-www-form-urlencoded; charset=UTF-8",
                           "-A", UA, "--max-time", "120", "-w", "HTTP=%{http_code}",
                          "-d", body], capture_output=True, text=True).stdout
    return resp


def main():
    email = sys.argv[1] if len(sys.argv) > 1 else os.environ["UKHO_EMAIL"]
    password = sys.argv[2] if len(sys.argv) > 2 else os.environ["UKHO_PASSWORD"]
    tmp = tempfile.mkdtemp(prefix="ukho_")
    jar, page = f"{tmp}/cj", f"{tmp}/p"
    verifier = secrets.token_urlsafe(64)
    challenge = base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).rstrip(b"=").decode()
    params = {"client_id": CLIENT_ID, "response_type": "code", "redirect_uri": REDIRECT,
              "response_mode": "query", "scope": SCOPE, "code_challenge": challenge,
              "code_challenge_method": "S256", "state": "s", "nonce": "n"}
    args = []
    for k, v in params.items():
        args += ["--data-urlencode", f"{k}={v}"]
    _, html = curl(jar, page + "1", ["-G", AUTH + "/oauth2/v2.0/authorize"] + args)
    st = settings_of(html, "authorize")
    body = post_self_asserted(jar, st["transId"], st["csrf"], request_type="ResponsePage", signInEmail=email)
    if '"status":"200"' not in body:
        raise SystemExit(f"email step failed: {body[:200]}")
    tx1 = urllib.parse.quote(st["transId"], safe="")
    _, html = curl(jar, page + "2", [f"{AUTH}/api/CombinedSigninAndSignup/confirmed"
                                     f"?csrf_token={st['csrf']}&tx={tx1}&p=B2C_1A_SignIn_SPA"])
    st = settings_of(html, "password page")
    body = post_self_asserted(jar, st["transId"], st["csrf"], request_type="ResponsePage",
                              signInName=email, password=password)
    if '"status":"200"' not in body:
        raise SystemExit(f"password step failed: {body[:200]}")
    hdrs, _ = curl(jar, page + "3", [f"{AUTH}/api/CombinedSigninAndSignup/confirmed"
                                     f"?csrf_token={st['csrf']}&tx={tx1}&p=B2C_1A_SignIn_SPA"])
    m = re.search(r"(?im)^location:\s*(\S+)", hdrs)
    if not m:
        raise SystemExit("no redirect after password")
    loc = m.group(1).strip()
    qs = urllib.parse.parse_qs(urllib.parse.urlparse(loc).query)
    if "code" not in qs:
        raise SystemExit("no code in redirect: " + loc[:250])
    r = subprocess.run(["curl", "-s", "-4", "-X", "POST", TOKEN_URL,
                        "-H", "Content-Type: application/x-www-form-urlencoded",
                        "--data-urlencode", "grant_type=authorization_code",
                        "--data-urlencode", f"client_id={CLIENT_ID}",
                        "--data-urlencode", "code=" + qs["code"][0],
                        "--data-urlencode", "redirect_uri=" + REDIRECT,
                        "--data-urlencode", "code_verifier=" + verifier,
                        "--max-time", "60"], capture_output=True, text=True)
    d = json.loads(r.stdout)
    if "access_token" not in d:
        raise SystemExit(f"token redemption failed: {r.stdout[:250]}")
    print(f'export UKHO_TOKEN="{d["access_token"]}"')
    print(f'export UKHO_REFRESH_TOKEN="{d["refresh_token"]}"')


if __name__ == "__main__":
    main()
