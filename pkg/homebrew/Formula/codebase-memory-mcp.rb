class CodebaseMemoryMcp < Formula
  desc "Fast code intelligence engine for AI coding agents"
  homepage "https://github.com/DeusData/codebase-memory-mcp"
  version "0.9.0"
  license "MIT"

  on_macos do
    on_arm do
      url "https://github.com/DeusData/codebase-memory-mcp/releases/download/v#{version}/codebase-memory-mcp-darwin-arm64.tar.gz"
      sha256 "faa02f0404230c451a9812230394481948f80183801fa5bf67044b41c2f25ed4"
    end
    on_intel do
      url "https://github.com/DeusData/codebase-memory-mcp/releases/download/v#{version}/codebase-memory-mcp-darwin-amd64.tar.gz"
      sha256 "6af3d02a27f589901fa763d3971089337bc8c9838bbed5d0cf543ca9f1a9e543"
    end
  end

  on_linux do
    on_arm do
      url "https://github.com/DeusData/codebase-memory-mcp/releases/download/v#{version}/codebase-memory-mcp-linux-arm64.tar.gz"
      sha256 "68a345d9a6842f02a3cb07e187b28bc38c4f3a22967f47fadbcd0757ba93a680"
    end
    on_intel do
      url "https://github.com/DeusData/codebase-memory-mcp/releases/download/v#{version}/codebase-memory-mcp-linux-amd64.tar.gz"
      sha256 "e2832a8d207c26beaa30efa6222ed4a37cb3f526ca4bee060bfbf336ed6fc679"
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
