# PMM Deployment Checklist

## Pre-Deploy Audit (Feb 1, 2026)

### ✅ Configuration Files

| File | Status | Notes |
|------|--------|-------|
| `.env.example` | ✅ | Complete with all required vars |
| `docker-compose.yml` | ✅ | Services: postgres, api, collector, dashboard |
| `railway.toml` | ✅ | 23 lines, correct structure |
| `vercel.json` | ❌ | Not present (using Next.js defaults) |
| `railway.json` | ❌ | Not present (good - avoids global override) |

### ✅ Dockerfiles

| Service | Status | Base Image |
|---------|--------|------------|
| `apps/api/Dockerfile` | ✅ | python:3.11-slim |
| `apps/collector/Dockerfile` | ✅ | python:3.11-slim |
| `apps/dashboard/Dockerfile` | ✅ | python:3.11-slim |
| `apps/web/` | N/A | Deployed to Vercel |

### ✅ Database Migrations

20 migration files in `migrations/`:

```
001-006: Core tables (markets, tokens, snapshots, etc.)
007-012: Features (movers cache, simulated data, etc.)
013: Users and subscriptions (with Polar)
014: Market volatility stats
015: Stripe to Polar migration
016: Trade volumes (WSS volume accumulation) ⚠️ NEEDS RUN
017: Arbitrage tables ⚠️ NEEDS RUN
```

**⚠️ Migrations 016 and 017 need to be run on Railway!**

---

## Deployment Steps

### 1. Push to Origin

```bash
cd /Users/nate/prediction-market-movers
git add .
git status  # Verify changes
git commit -m "feat(arbitrage): add MVP cross-platform arbitrage detection"
git push origin main
```

### 2. Deploy to Vercel (Frontend)

The Next.js app at `apps/web/` deploys automatically via Vercel Git integration.

**Verify:**
- [ ] Build succeeds in Vercel dashboard
- [ ] Environment variables set (NEXT_PUBLIC_API_URL, etc.)
- [ ] BetterAuth cookies working (check auth flows)

### 3. Deploy to Railway (Backend + Collector)

Railway should auto-deploy from GitHub. Monitor:

```bash
# Watch Railway logs
railway logs -f --service api
```

**Environment Variables Required:**
```
# Database
DATABASE_URL=postgresql://...

# Auth (BetterAuth uses Next.js, not FastAPI)
BETTER_AUTH_SECRET=...

# Polar Billing
POLAR_ACCESS_TOKEN=...
POLAR_WEBHOOK_SECRET=...
POLAR_ORGANIZATION_ID=...

# External APIs
KALSHI_API_KEY=... (optional for REST)
KALSHI_API_SECRET=... (optional for REST)
```

### 4. Run Migrations

Connect to Railway Postgres and run migrations:

```bash
# Option A: Railway CLI
railway connect postgresql
\i migrations/016_trade_volumes.sql
\i migrations/017_arbitrage_tables.sql

# Option B: Run all migrations
for f in migrations/*.sql; do
  echo "Running $f..."
  psql $DATABASE_URL -f "$f"
done
```

### 5. Verify Services

**API Health:**
```bash
curl https://api.predictionmarketmovers.com/health
# Expected: {"status": "healthy", "service": "api"}
```

**Collector Health:**
- Check WSS is receiving messages
- Verify snapshots are being written

**Dashboard:**
- Visit Streamlit dashboard
- Check volume shows ⚡ (WSS) indicator

---

## Post-Deploy Tests

### Volume Display
- [ ] Navigate to dashboard
- [ ] Verify markets show volume (not all $0)
- [ ] Check for ⚡ indicator (WSS source)

### Arbitrage Detection
- [ ] Check `/arbitrage/opportunities` endpoint
- [ ] Verify no opportunities initially (no pairs configured)
- [ ] Create test market pair
- [ ] Verify detection runs every 30s

### Auth & Billing
- [ ] Sign up flow works
- [ ] Polar checkout creates subscription
- [ ] Webhook updates user tier

---

## Known Issues / TODOs

### High Priority
- [ ] **Kalshi WebSocket** - Currently REST-only, missing real-time prices
- [ ] **Market pair matching** - Manual only, fuzzy matching needs pg_trgm
- [ ] **BetterAuth migration** - Run `npx @better-auth/cli migrate` on Railway

### Medium Priority
- [ ] Dashboard still uses localStorage auth (should use BetterAuth session)
- [ ] Arbitrage API needs Pro+ tier check
- [ ] Rate limiting on arbitrage endpoints

### Low Priority
- [ ] SEO: Market landing pages
- [ ] Weekly movers blog posts
- [ ] Order book depth storage

---

## Rollback Plan

If deployment fails:

1. **Vercel:** Instant rollback in dashboard
2. **Railway:** Redeploy previous commit
3. **Database:** Migrations are additive (no destructive changes)

---

## Contacts

- **Railway Dashboard:** https://railway.app/project/...
- **Vercel Dashboard:** https://vercel.com/...
- **Polar Dashboard:** https://polar.sh/...

Last updated: Feb 1, 2026
