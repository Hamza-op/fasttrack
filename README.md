# ⚡ FastTrack

FastTrack is a high-performance Windows desktop application built with Rust and Slint, designed for rapid and secure camera-card ingestion into structured client/event workflows.

![FastTrack UI](file:///C:/Users/User/.gemini/antigravity/brain/71771b95-c7e3-4571-9ede-ebbd30d318a0/media__1773792670466.png)

## 🚀 Key Features

- **Blazing Fast Ingest**: Optimized for high-speed data transfer from media cards.
- **Smart Routing**: Automatically organizes media into `Photos/RAW`, `Photos/JPG`, `Videos`, and `Audio`.
- **Data Integrity**: Integrated checksum verification for every file copied.
- **Modern UI**: Sleek, dark-mode-only interface with real-time status tracking.
- **Single Instance**: Guaranteed single-process execution on Windows to prevent data corruption.
- **Resume Support**: Gracefully handles interruptions and resumes ingest where it left off.

## 🛠 Project Structure

- `src/app/ingest.rs`: Core ingestion engine with verification logic.
- `src/app/scanner.rs`: Media detection and layout parsing.
- `ui/app.slint`: Modern Slint-based declarative UI.
- `.github/workflows/release.yml`: Automated CI/CD for GitHub Releases.

## 📦 Getting Started

### Prerequisites

- Windows 10/11
- [Rust Toolchain](https://rustup.rs/)

### Build & Run

```powershell
# Build for development
cargo run

# Build optimized release executable
cargo build --release
```

The release executable will be located at `target\release\fasttrack.exe`.

## 🤖 Automated Releases

This project uses GitHub Actions to publish a rolling GitHub prerelease whenever you push to `main` or `master`.

1. Push your commit: `git push origin main`
2. GitHub Actions builds the app on Windows
3. The latest `fasttrack.exe` is uploaded to the `auto-release` prerelease

If you want versioned releases later, keep this workflow for continuous builds and add a second tag-based release workflow.

---
*Developed by Hamza-op*
