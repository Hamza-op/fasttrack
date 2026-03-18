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

This project uses GitHub Actions to automate releases. To trigger a new release:
1. Tag your commit: `git tag v1.0.0`
2. Push the tag: `git push origin v1.0.0`
3. GitHub will automatically build and upload the `fasttrack.exe` to a new Release.

---
*Developed by Hamza-op*
