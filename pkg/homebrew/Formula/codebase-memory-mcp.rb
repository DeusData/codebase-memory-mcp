class CodebaseMemoryMcp < Formula
  desc "Fast code intelligence engine for AI coding agents"
  homepage "https://github.com/DeusData/codebase-memory-mcp"
  version "0.10.3"
  license "MIT"

  on_macos do
    on_arm do
      url "https://github.com/DeusData/codebase-memory-mcp/releases/download/v#{version}/codebase-memory-mcp-darwin-arm64.tar.gz"
      sha256 "0ebf02328207d4c3d862c837b5e973de5bac808df92b0941737721d467287f7f"
    end
    on_intel do
      url "https://github.com/DeusData/codebase-memory-mcp/releases/download/v#{version}/codebase-memory-mcp-darwin-amd64.tar.gz"
      sha256 "1107fea28285823e1436e4f38a4e00a0b472d8a43c379da7dfd200c914a4b9dd"
    end
  end

  on_linux do
    on_arm do
      url "https://github.com/DeusData/codebase-memory-mcp/releases/download/v#{version}/codebase-memory-mcp-linux-arm64.tar.gz"
      sha256 "967b9eababfdbd2ef1987c571d55bc7c028cd1db7f99279830634c58db311e32"
    end
    on_intel do
      url "https://github.com/DeusData/codebase-memory-mcp/releases/download/v#{version}/codebase-memory-mcp-linux-amd64.tar.gz"
      sha256 "74997fb0934e70a22f20c2e112fb4d883867dc1f01a7bcdc94cf86d13b5cbd31"
    end
  end

  def install
    bin.install "codebase-memory-mcp"
    # Third-party attribution bundle (present in archives since v0.8.1)
    doc.install "THIRD_PARTY_NOTICES.md" if File.exist?("THIRD_PARTY_NOTICES.md")
  end

  def caveats
    <<~EOS
      Run the following to configure your coding agents:
        codebase-memory-mcp install

      To tap this formula directly:
        brew tap deusdata/codebase-memory-mcp https://github.com/DeusData/codebase-memory-mcp
        brew install codebase-memory-mcp
    EOS
  end

  test do
    assert_match "codebase-memory-mcp", shell_output("#{bin}/codebase-memory-mcp --version")
  end
end
