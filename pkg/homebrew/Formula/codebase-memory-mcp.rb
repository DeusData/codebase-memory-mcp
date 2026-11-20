class CodebaseMemoryMcp < Formula
  desc "Fast code intelligence engine for AI coding agents"
  homepage "https://github.com/DeusData/codebase-memory-mcp"
  version "0.12.1"
  license "MIT"

  on_macos do
    on_arm do
      url "https://github.com/DeusData/codebase-memory-mcp/releases/download/v#{version}/codebase-memory-mcp-darwin-arm64.tar.gz"
      sha256 "9bfcbe38414b47969a3e1d1c1999f5357409387c86fa3d24221b5458267660d2"
    end
    on_intel do
      url "https://github.com/DeusData/codebase-memory-mcp/releases/download/v#{version}/codebase-memory-mcp-darwin-amd64.tar.gz"
      sha256 "ca6e64397a8ae80cec898a8205a6aa16750ac798acc05a0c3ed983837944a581"
    end
  end

  # The static -portable build: the dynamic one needs glibc 2.38, which
  # Ubuntu 22.04, Debian 12 and RHEL 9 do not have.
  on_linux do
    on_arm do
      url "https://github.com/DeusData/codebase-memory-mcp/releases/download/v#{version}/codebase-memory-mcp-linux-arm64-portable.tar.gz"
      sha256 "ed0f1f548cd79b121c401c2e9f7841c189a001af26d3768368088c24f6a53533"
    end
    on_intel do
      url "https://github.com/DeusData/codebase-memory-mcp/releases/download/v#{version}/codebase-memory-mcp-linux-amd64-portable.tar.gz"
      sha256 "8c3d0341064301e78ecc84cf7348d4e2c6562a0f57448243ea560aaa34de36c1"
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
