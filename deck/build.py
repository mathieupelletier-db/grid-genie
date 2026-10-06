"""Assemble the Grid Genie business deck: catalog CSS + slide sections + inlined runtime JS."""
import re
from pathlib import Path

RES = Path.home() / ".vibe/marketplace/plugins/fe-html-slides/skills/html-slides/resources"
HERE = Path(__file__).parent

catalog = (RES / "pattern-catalog.html").read_text()
css = re.search(r"<style>(.*?)</style>", catalog, re.S).group(1)
sections = (HERE / "src/sections.html").read_text()
js = lambda name: (RES / name).read_text().replace("</script>", "<\\/script>")

html = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Grid Genie · Fewer outage hours, ready before winter</title>
  <style>{css}</style>
</head>
<body>
  <deck-stage width="1920" height="1080">
{sections}
  </deck-stage>
  <script>{js("deck-stage.js")}</script>
  <script>{js("image-slot.js")}</script>
</body>
</html>
"""
out = HERE / "grid-genie-business-deck.html"
out.write_text(html)
print(out, len(html) // 1024, "KB")
