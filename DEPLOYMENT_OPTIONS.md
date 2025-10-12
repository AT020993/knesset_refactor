# Knesset Data Platform - Deployment Options

**Version:** 1.0 | **Date:** October 2025

---

## Executive Summary

Deployment options for the **Knesset Parliamentary Data Analysis Platform** for non-technical users.

**Key Requirements**: Simple access, no coding required, secure, minimal maintenance, cost-effective

**Recommended**: Start with **Option 1 (Streamlit Community Cloud)**, upgrade based on usage and privacy needs

---

## Platform Overview

**Capabilities**: 21+ visualizations, automated data collection, self-service interface, multi-format export
**Status**: 48MB database, 60%+ test coverage, CI/CD via GitHub Actions, GCS sync implemented

---

## Deployment Options

### Option 1: Streamlit Community Cloud ⭐ RECOMMENDED

**Overview**: Cloud hosting for data apps. Users access via web URL (`https://knesset-data.streamlit.app`)

**How It Works**: Auto-deploys from GitHub, browser access, data persists in GCS, automatic updates, zero installation

**Pros**: FREE (or $20/month private), zero maintenance, instant deployment, HTTPS security, auto-updates, cross-platform
**Cons**: Public by default ($20 for privacy), 1GB RAM limit, spindown after 7 days inactivity, shared infrastructure

**Cost**:
| Tier | Monthly | Features |
|------|---------|----------|
| Public | $0 | Public access, 1GB RAM |
| Private | $20 | Private access, 1GB RAM |
| Teams | $42 | Private, 4GB RAM, SSO, no spindown |

**Setup**: 30 minutes | **Maintenance**: 1 hour/month

**Best For**: Quick deployment, budget-conscious, distributed teams, public research data

---

### Option 2: Institutional Server (Self-Hosted)

**Overview**: Platform on internal server, accessible within organizational network

**How It Works**: IT provisions server, platform installed, 24/7 service, internal URL, data stored locally

**Pros**: Complete data control, network isolation, institutional integration, offline capable, dedicated resources, no subscription
**Cons**: Requires IT resources, manual updates, VPN for remote access, backup responsibility, security maintenance, 2-4 days setup

**Cost**: Initial 16-32 IT hours + ongoing 2-4 hours/month

**Setup**: 3-5 days | **Maintenance**: 2-4 hours/month

**Best For**: Strict privacy, dedicated IT support, on-campus usage, institutional integration

---

### Option 3: Streamlit Cloud for Teams (Professional)

**Overview**: Premium cloud with enhanced resources and privacy. Same simplicity as Option 1, enterprise features

**How It Works**: Like Option 1 with 4GB RAM, private deployment, no spindown, SSO integration

**Pros**: All Option 1 benefits + private, better performance (4GB), no spindown, team management, priority support, SSO, custom branding
**Cons**: $42/month cost, cloud-hosted, less control than self-hosted

**Cost**: $42/month ($504/year)

**Setup**: 45 minutes | **Maintenance**: 30 minutes/month

**Best For**: Professional environments, guaranteed uptime, private + accessible, $500/year budget

---

### Option 4: Cloud Platform Deployment (GCP/AWS/Azure)

**Overview**: Custom deployment using Docker containers. Maximum flexibility

**How It Works**: Docker packaging, deploy to cloud platform, auto-scaling, load balancing, monitoring

**Pros**: Full customization, scalability, cloud integration, load balancing, geographic deployment
**Cons**: High complexity (DevOps required), 1-2 weeks setup, ongoing management, $30-100/month, learning curve

**Cost**: $10-40/month depending on platform

**Setup**: 1-2 weeks | **Maintenance**: 4-8 hours/month

**Best For**: DevOps teams, enterprise-scale, complex integrations, 100+ concurrent users

---

## Side-by-Side Comparison

| Criteria | Community Cloud | Institutional Server | Teams Cloud | Custom Cloud |
|----------|-----------------|---------------------|-------------|--------------|
| **Setup Time** | ⚡ 30 min | ⏰ 3-5 days | ⚡ 45 min | ⏰ 1-2 weeks |
| **Cost** | 💰 Free-$20/mo | 💰💰 Variable | 💰💰 $42/mo | 💰💰💰 $30-100/mo |
| **Maintenance** | ✅ 1 hour | ⚠️ 2-4 hours | ✅ 30 min | ⚠️ 4-8 hours |
| **Expertise** | ✅ None | ⚠️ IT staff | ✅ None | ❌ DevOps |
| **Privacy** | ⚠️ Cloud | ✅ On-premise | ⚠️ Cloud | ⚠️ Cloud |
| **Remote Access** | ✅ Anywhere | ⚠️ VPN | ✅ Anywhere | ✅ Anywhere |
| **Performance** | ⚠️ 1GB RAM | ✅ Configurable | ✅ 4GB RAM | ✅ Configurable |
| **Auto-updates** | ✅ Yes | ❌ Manual | ✅ Yes | ⚠️ Configurable |

**Legend**: ✅ Excellent | ⚠️ Moderate | ❌ Challenging | 💰 Low | 💰💰 Medium | 💰💰💰 High

---

## Total Cost of Ownership (1 Year)

| Option | Setup | Monthly | Annual Total | Notes |
|--------|-------|---------|--------------|-------|
| Community (Public) | $0 | $0 | **$0** | None |
| Community (Private) | $0 | $20 | **$240** | None |
| Institutional Server | Variable | Variable | **$2,000-5,000** | IT time + infrastructure |
| Teams Cloud | $0 | $42 | **$504** | None |
| Custom Cloud | Variable | $50 | **$600-1,200** | DevOps time |

---

## Decision Matrix

**Choose Community Cloud If**: Immediate deployment, limited budget ($0-240/year), cloud OK, zero maintenance, easy remote access

**Choose Institutional Server If**: Strict privacy, dedicated IT support, on-campus usage, complete control, institutional integration

**Choose Teams Cloud If**: Professional reliability, $500/year budget, guaranteed performance, team management + SSO, simplicity + privacy

**Choose Custom Cloud If**: DevOps available, enterprise-scale, complex integrations, 100+ users, maximum customization

---

## Phased Approach (Recommended)

### Phase 1: Pilot (Months 1-3) - Option 1
- Deploy on Community Cloud ($0-20/month)
- Onboard 5-10 users, gather feedback
- Assess usage patterns and requirements
- **Budget**: $0-60

### Phase 2: Evaluate (Month 3)
- Privacy concerns? → Option 2
- Performance critical? → Option 3
- Current working well? → Stay Option 1
- Institutional integration? → Option 2

### Phase 3: Scale (Months 4+)
- Most common: Upgrade to Option 3 ($42/month)
- Privacy-critical: Migrate to Option 2
- Budget-constrained: Continue Option 1

---

## User Experience (All Options)

**Same interface regardless of deployment:**

1. Open browser → Navigate to URL
2. Dashboard with sidebar navigation
3. Features: Data refresh, predefined queries, 21+ visualizations, CSV/Excel export, table explorer, SQL sandbox
4. Workflow: `Select Analysis → Apply Filters → View Results → Export`

**No Python, no command line, no technical skills required**

---

## Next Steps

### Option 1 (Recommended):
1. Confirm budget ($0 public / $20 private)
2. Setup (30 min): Create GCS bucket, configure credentials, deploy, test
3. Onboarding: User guide, 30-min training, share URL
4. **Timeline**: Week 1 setup, Week 2 onboarding, Week 3 production

### Option 2 (Institutional Server):
1. Confirm IT support and budget
2. Week 1: Meet IT, define requirements, schedule provisioning
3. Weeks 2-3: Server setup, installation, security testing
4. Week 4: Training, rollout, monitoring

---

## Questions for Management

1. Privacy: Is data sensitive? (Public vs private deployment)
2. Budget: Acceptable annual cost? ($0 - $500 - $5,000+)
3. Timeline: How quickly needed? (30 minutes vs 3-5 days)
4. Support: IT resources available? (Self-hosted vs cloud)
5. Access: Remote access required? (VPN vs web)
6. Scale: Number of users? (5, 20, 100+)
7. Integration: Need institutional systems? (LDAP, SSO)

---

## Summary

**Recommendation**: Start with **Option 1 (Streamlit Community Cloud)** at $0-20/month for 3-month pilot

**Why**: Fastest time to value (30 min), lowest risk ($0-60 pilot cost), validates usage, easy migration, immediate access

**After 3 months**: Evaluate and upgrade to Option 2 (server) or Option 3 (teams) if needed

**This minimizes risk while maximizing learning**

---

*Last updated: October 2025*
