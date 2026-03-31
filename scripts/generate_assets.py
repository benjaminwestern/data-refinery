from __future__ import annotations

from html import escape
from pathlib import Path
from textwrap import dedent

ROOT = Path(__file__).resolve().parents[1]
ASSETS_DIR = ROOT / "assets"

PALETTE = {
    "paper": "#F4EFE6",
    "paper_alt": "#FBF7F0",
    "ink": "#18232A",
    "ink_alt": "#293A44",
    "line": "#D6C7B3",
    "shadow": "#BDAE99",
    "text": "#FFF9F1",
    "muted": "#D9CEC0",
    "dark_text": "#223039",
    "accent": "#C8643B",
    "teal": "#3B6F73",
    "gold": "#C29A3A",
    "sage": "#5D7056",
    "brick": "#9F4A3F",
    "blue": "#476C93",
}

FONT_MONO = (
    '"JetBrainsMono Nerd Font", "Hack Nerd Font", "CaskaydiaMono Nerd Font", '
    '"SFMono-Regular", Menlo, Monaco, Consolas, "Liberation Mono", monospace'
)


def write_asset(filename: str, content: str) -> None:
    (ASSETS_DIR / filename).write_text(content, encoding="utf-8")


def layered_card(
    x: int,
    y: int,
    width: int,
    height: int,
    radius: int,
    fill: str,
    *,
    stroke: str,
    shadow_dx: int = 8,
    shadow_dy: int = 8,
) -> str:
    return dedent(
        f"""\
        <rect x="{x + shadow_dx}" y="{y + shadow_dy}" width="{width}" height="{height}" rx="{radius}" fill="{PALETTE["shadow"]}" />
        <rect x="{x}" y="{y}" width="{width}" height="{height}" rx="{radius}" fill="{fill}" stroke="{stroke}" stroke-width="2" />
        """
    )


def create_banner() -> str:
    return dedent(
        f"""\
        <svg width="1280" height="380" viewBox="0 0 1280 380" fill="none" xmlns="http://www.w3.org/2000/svg">
          <defs>
            <pattern id="ledger" width="48" height="48" patternUnits="userSpaceOnUse">
              <path d="M 48 0 L 0 0 0 48" fill="none" stroke="{PALETTE["line"]}" stroke-width="1" />
            </pattern>
            <clipPath id="frame-clip">
              <rect width="1280" height="380" rx="28" />
            </clipPath>
            <style><![CDATA[
              .badge {{ font-family: {FONT_MONO}; font-size: 13px; font-weight: 700; fill: {PALETTE["dark_text"]}; }}
              .eyebrow {{ font-family: {FONT_MONO}; font-size: 15px; font-weight: 700; letter-spacing: 0.14em; fill: {PALETTE["accent"]}; }}
              .title {{ font-family: {FONT_MONO}; font-size: 58px; font-weight: 700; fill: {PALETTE["text"]}; }}
              .subtitle {{ font-family: {FONT_MONO}; font-size: 19px; font-weight: 700; fill: {PALETTE["muted"]}; }}
              .meta {{ font-family: {FONT_MONO}; font-size: 14px; fill: {PALETTE["muted"]}; }}
              .footer {{ font-family: {FONT_MONO}; font-size: 14px; font-weight: 700; fill: {PALETTE["dark_text"]}; }}
              .panel-label {{ font-family: {FONT_MONO}; font-size: 12px; font-weight: 700; letter-spacing: 0.1em; fill: {PALETTE["accent"]}; }}
              .panel-title {{ font-family: {FONT_MONO}; font-size: 16px; font-weight: 700; fill: {PALETTE["dark_text"]}; }}
              .panel-copy {{ font-family: {FONT_MONO}; font-size: 15px; fill: {PALETTE["dark_text"]}; }}
              .panel-copy-muted {{ font-family: {FONT_MONO}; font-size: 14px; fill: {PALETTE["ink_alt"]}; }}
            ]]></style>
          </defs>

          <g clip-path="url(#frame-clip)">
            <rect width="1280" height="380" rx="28" fill="{PALETTE["paper"]}" />
            <rect width="1280" height="380" rx="28" fill="url(#ledger)" opacity="0.72" />
            <rect x="0" y="318" width="1280" height="62" fill="{PALETTE["paper_alt"]}" />

            {layered_card(42, 34, 762, 266, 22, PALETTE["ink"], stroke="#30414B").strip()}
            <rect x="70" y="58" width="170" height="32" rx="9" fill="{PALETTE["paper"]}" />
            <text x="86" y="79" class="badge">&gt; data-refinery</text>
            <rect x="70" y="112" width="28" height="3" rx="1.5" fill="{PALETTE["accent"]}" />
            <text x="114" y="118" class="eyebrow">INGEST  ANALYSE  REWRITE</text>
            <text x="70" y="176" class="title">Data Refinery</text>
            <text x="70" y="216" class="subtitle">normalise raw files into stable datasets</text>
            <text x="70" y="244" class="subtitle">inspect JSON workflows before you mutate them</text>
            <text x="70" y="272" class="subtitle">apply preview-first cleanup with local or GCS paths</text>

            {layered_card(844, 48, 364, 122, 18, PALETTE["paper_alt"], stroke=PALETTE["line"], shadow_dx=6, shadow_dy=6).strip()}
            <text x="868" y="72" class="panel-label">[ surfaces ]</text>
            <text x="868" y="98" class="panel-title">supported workflows</text>
            <text x="868" y="124" class="panel-copy">ingest   analysis   rewrite</text>
            <text x="868" y="148" class="panel-copy-muted">local paths and gs:// URIs</text>

            {layered_card(844, 188, 364, 118, 18, PALETTE["paper_alt"], stroke=PALETTE["line"], shadow_dx=6, shadow_dy=6).strip()}
            <text x="868" y="212" class="panel-label">[ outputs ]</text>
            <text x="868" y="238" class="panel-title">normalised and reviewable</text>
            <text x="868" y="264" class="panel-copy">csv  json  ndjson  jsonl</text>
            <text x="868" y="288" class="panel-copy-muted">plus reports, summaries, backups</text>

            <rect x="816" y="60" width="10" height="226" rx="5" fill="{PALETTE["gold"]}" />
            <rect x="1224" y="62" width="16" height="16" rx="3" fill="{PALETTE["teal"]}" />
            <rect x="1224" y="88" width="16" height="16" rx="3" fill="{PALETTE["accent"]}" />
            <rect x="1224" y="114" width="16" height="16" rx="3" fill="{PALETTE["sage"]}" />

            <text x="70" y="354" class="footer">workflow-first CLI  •  config-driven jobs  •  preview before apply</text>
          </g>
        </svg>
        """
    )


def create_header(text: str, accent: str) -> str:
    label = escape(text)
    card_width = max(360, min(560, 188 + len(text) * 16))
    return dedent(
        f"""\
        <svg width="920" height="82" viewBox="0 0 920 82" fill="none" xmlns="http://www.w3.org/2000/svg">
          <defs>
            <style><![CDATA[
              .label {{ font-family: {FONT_MONO}; font-size: 23px; font-weight: 700; fill: {PALETTE["dark_text"]}; }}
              .meta {{ font-family: {FONT_MONO}; font-size: 11px; font-weight: 700; letter-spacing: 0.14em; fill: {accent}; }}
            ]]></style>
          </defs>
          <rect x="8" y="14" width="{card_width}" height="48" rx="14" fill="{PALETTE["shadow"]}" />
          <rect x="0" y="6" width="{card_width}" height="48" rx="14" fill="{PALETTE["paper_alt"]}" stroke="{PALETTE["line"]}" stroke-width="2" />
          <rect x="22" y="0" width="112" height="18" rx="6" fill="{accent}" />
          <text x="33" y="13" class="meta">SECTION</text>
          <rect x="24" y="30" width="26" height="3" rx="1.5" fill="{accent}" />
          <text x="62" y="35" class="label" dominant-baseline="middle">{label}</text>
        </svg>
        """
    )


def main() -> None:
    ASSETS_DIR.mkdir(parents=True, exist_ok=True)

    write_asset("banner.svg", create_banner())
    write_asset("header-overview.svg", create_header("Overview", PALETTE["accent"]))
    write_asset("header-prerequisites.svg", create_header("Prerequisites", PALETTE["gold"]))
    write_asset("header-quick-start.svg", create_header("Quick start", PALETTE["teal"]))
    write_asset("header-workflows.svg", create_header("Workflows", PALETTE["blue"]))
    write_asset("header-capabilities.svg", create_header("What it can and can't do", PALETTE["brick"]))
    write_asset("header-cli-surface.svg", create_header("CLI and package surface", PALETTE["gold"]))
    write_asset("header-configuration.svg", create_header("Configuration and examples", PALETTE["sage"]))
    write_asset("header-diagrams.svg", create_header("Behaviour diagrams", PALETTE["brick"]))
    write_asset("header-development.svg", create_header("Development", PALETTE["ink"]))
    write_asset("header-roadmap.svg", create_header("Roadmap", PALETTE["accent"]))


if __name__ == "__main__":
    main()
