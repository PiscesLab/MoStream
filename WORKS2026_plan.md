# WORKS 2026 — MoStream Paper Plan & Tracker

> **Living document.** Tick boxes as we go. Update the "Status" line each working day.
> Source of truth for the paper effort — if it's not here, it's at risk of being forgotten.

---

## ⏰ The clock

| | |
|---|---|
| **Venue** | WORKS 2026 — 21st Workshop on Workflows in Support of Large-Scale Science |
| **Co-located with** | SC26 — Chicago, McCormick Place, Nov 15 2026 |
| **Site** | https://works-workshop.org/ |
| **PAPER DEADLINE** | **July 31, 2026 — Anywhere on Earth** |
| Notification | Sept 4, 2026 |
| Camera-ready | Sept 25, 2026 |
| **Our target submit date** | **July 30, 2026** (Jul 31 AoE = disaster buffer only) |

**Today: Tue Jul 7, 2026. We start Wed Jul 8. ~23 days to deadline.**

**Status (update daily):** _Plan drafted Jul 7. Not yet started._

---

## 🔒 Locked decisions (Jul 7)

- [x] **Submission type:** full paper, **≤ 8 pages**, IEEE two-column US-letter template. Page limit **includes** figures, tables, references, appendices. **DOIs required** in references.
- [x] **Contribution framing:** **systems-primary** — MoStream as a fault-tolerant streaming active-learning *workflow*; novelty = sustaining stateful ML training in Flink/Beam despite worker recycling + memory pressure.
- [x] **Evaluation:** cluster is **up**; we run **new** experiments (E1–E6 below).

---

## 🎯 The paper

**Working title:** *MoStream: Sustaining a Stateful Streaming Active-Learning Workflow for Molecular Discovery on Apache Flink*
_(alt: "Keeping the Loop Alive: Fault-Tolerant Streaming Active Learning on Flink/Beam")_

**Thesis:** stream dataflow engines (Flink/Beam) are an attractive substrate for continuous, learn-in-the-loop scientific workflows — but they were **not** built to host long-lived, stateful, polyglot ML. MoStream shows the workflow architecture and the systems techniques that make it survive in practice.

**Contributions (= abstract skeleton):**
1. A **streaming workflow** closing the active-learning loop (sim → Kafka → train → infer → rank → recommend → sim) as a continuous dataflow rather than a batch scheduler.
2. **Surviving runtime worker recycling** — persisting model weights so Beam's ~10-min Python-env recycle doesn't reset training; bundle-sizing to trade recycle frequency vs. latency.
3. **Memory budgeting for polyglot streaming ML** — characterizing where off-heap/direct memory goes (TF JNI + Beam gRPC/Netty) and a budget that turns an ~18h OOM crash into multi-day stability.
4. **Evaluation** on a 6-node CloudLab cluster: long-run stability, scaling, throughput/latency, recovery cost, and that the AL loop still converges through all of it.

**Section budget (IEEE 2-col, 8 pp):**

| § | Section | Pages |
|---|---------|-------|
| 1 | Introduction | 1.0 |
| 2 | Background & Motivation | 0.75 |
| 3 | System Design + **Fig 1 (architecture)** | 1.75 |
| 4 | Sustaining Stateful Streaming ML *(the meat — from `cloudlab/notes.md`)* | 1.75 |
| 5 | Evaluation | 2.0 |
| 6 | Related Work | 0.5 |
| 7 | Lessons Learned / Discussion | 0.25 |
| 8 | Conclusion & Future Work | 0.25 |

_Reproducibility → point to the public `scripts/` repo (appendices count against the 8 pp)._

---

## 🧪 Experiment matrix

| ID | Experiment | Backs claim | Configs | Wall-clock | Done? |
|----|-----------|-------------|---------|-----------|:---:|
| **E1** | Long-run stability | "NO OOM over multi-day" headline | tuned, P=8, 48h | **48h — start Jul 8** | [ ] |
| **E2** | Memory budgeting A/B | direct-mem fix: crash → stable | off-heap 512m (before) vs 1024m (after) | before crashes ~18h — **start Jul 8** | [ ] |
| **E3** | Scaling sweep | throughput/latency vs parallelism | P = 1, 2, 4, 8 (,16) | ~1–2h each | [ ] |
| **E4** | Recovery cost | recycle downtime + fault recovery | bundle.size 1 vs 100; kill-a-worker | ~1–2h | [ ] |
| **E5** | AL efficacy | loop converges + recommends *better* molecules | full loop | few h | [ ] |
| **E6** | Weight persistence | model survives recycle | with vs without persist | ~1h | [ ] |

> **Critical path = E1 + E2-before** (calendar time you can't compress). Launch both Day 1.
> Most persuasive *systems* figures: E2 before/after and E6 with/without.

---

## 🗓️ Timeline

### Phase 0 — Kickoff & de-risk · Wed Jul 8 – Thu Jul 9
- [ ] Renew CloudLab reservation; health-check all 6 nodes
- [ ] 30-min end-to-end smoke test (sim → Kafka → Flink → Recommend offset growing)
- [ ] Commit pending tooling (`plot_heap.py`, `monitor_taskmanager.sh`); revert `MDWorkflow.py` whitespace no-op
- [ ] **Launch E1 (48h) + E2-before (18h)** ← do this first
- [ ] Create Overleaf IEEE 2-col project; paste section skeleton + contribution bullets; start `.bib`

### Phase 1 — Results-free writing + short runs · Fri Jul 10 – Thu Jul 16
- [ ] Harvest E1 / E2; run E3 (scaling) + E6 (persistence)
- [ ] Write §1 Intro
- [ ] Write §2 Background & Motivation
- [ ] Write §3 System Design; turn `MoStream_architecture.html` → **Figure 1**
- [ ] 🏁 **Jul 16 milestone:** §1–3 drafted; E1/E2/E3/E6 data in hand; Fig 1 done

### Phase 2 — Core section + evaluation · Fri Jul 17 – Thu Jul 23
- [ ] Run E4 (recovery / fault-injection) + E5 (AL convergence + recommendation quality)
- [ ] Write §4 Sustaining Stateful Streaming ML (lift from `cloudlab/notes.md`)
- [ ] Write §5 Evaluation; finalize all result figures
- [ ] Write §6 Related Work (Pegasus/Parsl/Nextflow/Swift; streaming AL; ML-on-Flink; in-situ/in-transit)
- [ ] 🏁 **Jul 23 milestone:** complete rough full draft — every section, every figure

### Phase 3 — Review, tighten, polish · Fri Jul 24 – Wed Jul 29
- [ ] **Jul 24:** send full draft to advisor/co-authors — **review window #1**
- [ ] Revise; cut to 8 pp; grayscale-safe, legible figures
- [ ] Write §7 Lessons + §8 Conclusion + final Abstract
- [ ] References pass — add **DOIs**, fix citations
- [ ] **Jul 28: review window #2** (advisor final read)
- [ ] Jul 29: final revisions, proofread, IEEE-format + page-limit compliance check

### Phase 4 — Submit early · Thu Jul 30
- [ ] Submission-site dry run; register submission
- [ ] Upload PDF; **SUBMIT**
- [ ] Jul 31 AoE held as pure buffer

---

## 🔥 Tomorrow (Jul 8) — first 5 moves
1. [ ] Renew CloudLab reservation + health-check 6 nodes
2. [ ] Commit pending tooling; revert the `MDWorkflow.py` whitespace no-op
3. [ ] **Launch E1 (48h) and E2-before (18h) runs**
4. [ ] Spin up the Overleaf IEEE project + paste skeleton
5. [ ] Write the abstract paragraph + 4 contribution bullets (align with advisor before prose)

---

## ⚠️ Risks & mitigations
- **CloudLab expiry / NFS resets** → renew reservation Jul 8; keep model/data on `/mnt/media` (not NFS); snapshot configs.
- **Long runs eat calendar** → E1/E2 start Day 1; a failed run still leaves ~3 weeks to redo.
- **Page overflow** (every systems paper) → dedicated tighten pass Jul 25–27; reproducibility lives in external `scripts/` repo.
- **Advisor availability** → book Jul 24 & Jul 28 review windows *now*.
- **Weakest scientific claim = "does AL actually help" (E5)** → frame as *supporting* result, not headline; the stability story stands alone.

---

## 📦 Assets we already have (don't rebuild)
- `cloudlab/notes.md` — systems findings, reads like a §4 draft (OOM/recycle/memory fixes = the contributions)
- `MoStream/MDStream/StreamML/Discussion.md` — "OOM solved; favor systems novelty"
- `MoStream_architecture.html` — basis for **Figure 1**
- `plot_heap.py` (heap / direct / loss / combined modes) + `monitor_taskmanager.sh` — the evaluation data + figure pipeline
- `scripts/` (public) — reproducibility artifact
- `LogFlink/` (Jun 23) — existing multi-hour no-OOM run + loss/heap/direct logs (fallback data)
- Memory: `project_works2026_paper.md`, `project_mostream_cloudlab.md`, `project_mostream_issues.md`

---

_Last updated: Jul 7, 2026._
