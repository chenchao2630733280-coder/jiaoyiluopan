"""Markdown → Word(.docx) 导出（report_docx.py）。

为什么不用 python-docx：U 盘便携版是 embeddable Python，没有 pip，任何第三方库在
现场都会当场失效。.docx 本质就是一个 zip 包着几份 OOXML，标准库的 zipfile 足够，
所以这里手写一个最小生成器，只支持前端真正会产出的那几种 Markdown 语法：

    # / ## / ###      标题（带 outlineLvl，Word 导航窗格里能看到层级）
    > 文字           引用块（灰字加缩进）
    - 文字           无序列表（用「•」前缀 + 悬挂缩进，不引 numbering.xml）
    | 表格 | 行 |      管道表格，第二行若是 |---| 则当作表头分隔
    **加粗**          行内粗体
    其余非空行         普通段落

OOXML 的坑（都已踩平，改这里之前先看一眼）：
1. `pPr` / `rPr` 的子元素**顺序是强约束**，Word 对顺序错的文件直接报「无法打开」，
   不是渲染差异。这里严格按 CT_PPr / CT_RPr 的 sequence 排：spacing→ind→jc→outlineLvl，
   rFonts→b→color→sz。
2. 表格必须先 `tblGrid` 再 `tr`，每个 `tc` 必须至少含一个 `p`（空单元格也要有空段落）。
3. 每个 zip 部件都要在 `[Content_Types].xml` 里登记，缺一个就是「文件已损坏」。
4. 中文要显式给 `w:eastAsia`，否则 Word 会拿西文字体去排中文，字号看着发虚。
"""
import io
import re
import time
import unicodedata
import zipfile

NS = ('xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main" '
      'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"')

PAGE_W = 9000          # 正文可用宽度（twips）≈ A4 减 2×1 英寸页边距
FONT_CN = "宋体"
FONT_HEAD = "黑体"
FONT_EN = "Calibri"


def _esc(s):
    return (str(s).replace("&", "&amp;").replace("<", "&lt;")
            .replace(">", "&gt;").replace('"', "&quot;"))


def _w(s):
    """按显示宽度估字符数：中文/全角算 2，西文算 1。"""
    n = 0
    for ch in str(s):
        n += 2 if unicodedata.east_asian_width(ch) in ("W", "F") else 1
    return max(1, n)


def _runs(text, bold=False, color=None, sz=None, font=None):
    """把 **加粗** 拆成多个 run；其余原样。"""
    parts = re.split(r"(\*\*[^*]+\*\*)", text)
    out = []
    for p in parts:
        if not p:
            continue
        b = bold
        if p.startswith("**") and p.endswith("**") and len(p) > 4:
            p = p[2:-2]
            b = True
        props = []
        f = font or FONT_CN
        props.append('<w:rFonts w:ascii="%s" w:hAnsi="%s" w:eastAsia="%s"/>' % (FONT_EN, FONT_EN, f))
        if b:
            props.append("<w:b/>")
        if color:
            props.append('<w:color w:val="%s"/>' % color)
        if sz:
            props.append('<w:sz w:val="%d"/><w:szCs w:val="%d"/>' % (sz, sz))
        out.append('<w:r><w:rPr>%s</w:rPr><w:t xml:space="preserve">%s</w:t></w:r>'
                   % ("".join(props), _esc(p)))
    return "".join(out)


def _p(text="", bold=False, sz=None, color=None, font=None, align=None,
       ind=None, before=0, after=60, outline=None, hang=None):
    pp = []
    if before or after:
        pp.append('<w:spacing w:before="%d" w:after="%d" w:line="300" w:lineRule="auto"/>'
                  % (before, after))
    if ind or hang:
        attrs = ""
        if ind:
            attrs += ' w:left="%d"' % ind
        if hang:
            attrs += ' w:hanging="%d"' % hang
        pp.append("<w:ind%s/>" % attrs)
    if align:
        pp.append('<w:jc w:val="%s"/>' % align)
    if outline is not None:
        pp.append('<w:outlineLvl w:val="%d"/>' % outline)
    body = _runs(text, bold=bold, color=color, sz=sz, font=font) if text else ""
    return "<w:p><w:pPr>%s</w:pPr>%s</w:p>" % ("".join(pp), body)


def _table(rows):
    ncol = max(len(r) for r in rows)
    rows = [r + [""] * (ncol - len(r)) for r in rows]
    # 列宽按每列最长内容分配，总宽固定，短列不会挤成一条线
    weights = []
    for c in range(ncol):
        weights.append(max(_w(r[c]) for r in rows) + 2)
    total = float(sum(weights))
    widths = [max(600, int(PAGE_W * w / total)) for w in weights]
    diff = PAGE_W - sum(widths)
    widths[-1] += diff
    grid = "".join('<w:gridCol w:w="%d"/>' % w for w in widths)

    borders = ('<w:tblBorders>'
               '<w:top w:val="single" w:sz="6" w:color="BFBFBF"/>'
               '<w:left w:val="single" w:sz="6" w:color="BFBFBF"/>'
               '<w:bottom w:val="single" w:sz="6" w:color="BFBFBF"/>'
               '<w:right w:val="single" w:sz="6" w:color="BFBFBF"/>'
               '<w:insideH w:val="single" w:sz="6" w:color="BFBFBF"/>'
               '<w:insideV w:val="single" w:sz="6" w:color="BFBFBF"/>'
               '</w:tblBorders>')
    out = ['<w:tbl><w:tblPr><w:tblW w:w="%d" w:type="dxa"/>%s'
           '<w:tblLayout w:type="fixed"/></w:tblPr><w:tblGrid>%s</w:tblGrid>'
           % (PAGE_W, borders, grid)]
    for ri, r in enumerate(rows):
        head = (ri == 0)
        cells = []
        for ci, cell in enumerate(r):
            shd = ('<w:shd w:val="clear" w:color="auto" w:fill="F2F2F2"/>' if head else "")
            cells.append('<w:tc><w:tcPr><w:tcW w:w="%d" w:type="dxa"/>%s'
                         '<w:vAlign w:val="center"/></w:tcPr>%s</w:tc>'
                         % (widths[ci], shd,
                            _p(cell, bold=head, sz=20, after=0)))
        out.append('<w:tr>%s</w:tr>' % "".join(cells))
    out.append("</w:tbl>")
    out.append(_p("", after=80))   # 表后留白，否则紧贴下一段
    return "".join(out)


def _flush_table(buf, lines):
    rows = []
    for ln in lines:
        s = ln.strip()
        if not s.startswith("|"):
            continue
        s = s.strip("|")
        cells = [c.strip() for c in s.split("|")]
        if all(re.match(r"^:?-{2,}:?$", c or "") for c in cells if c != "") and cells:
            continue          # 表头分隔行
        rows.append(cells)
    if rows:
        buf.append(_table(rows))


def md_to_body(md):
    buf = []
    tbl = []

    def close_tbl():
        if tbl:
            _flush_table(buf, tbl)
            del tbl[:]

    for raw in (md or "").splitlines():
        line = raw.rstrip()
        s = line.strip()
        if s.startswith("|"):
            tbl.append(s)
            continue
        close_tbl()
        if not s:
            continue
        if s.startswith("### "):
            buf.append(_p(s[4:], bold=True, sz=26, font=FONT_HEAD,
                          before=180, after=80, outline=2))
        elif s.startswith("## "):
            buf.append(_p(s[3:], bold=True, sz=30, font=FONT_HEAD,
                          before=240, after=100, outline=1))
        elif s.startswith("# "):
            buf.append(_p(s[2:], bold=True, sz=40, font=FONT_HEAD,
                          align="center", before=0, after=200))
        elif s.startswith("> "):
            buf.append(_p(s[2:], color="666666", sz=20, ind=240, after=40))
        elif re.match(r"^[-*]\s+", s):
            buf.append(_p("• " + re.sub(r"^[-*]\s+", "", s),
                          sz=21, ind=360, hang=180, after=30))
        elif re.match(r"^---+$", s):
            buf.append(_p("", after=60))
        else:
            buf.append(_p(s, sz=21, after=80))
    close_tbl()
    return "".join(buf)


def _styles():
    return ('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<w:styles %s>'
            '<w:docDefaults><w:rPrDefault><w:rPr>'
            '<w:rFonts w:ascii="%s" w:hAnsi="%s" w:eastAsia="%s"/>'
            '<w:sz w:val="21"/><w:szCs w:val="21"/>'
            '</w:rPr></w:rPrDefault>'
            '<w:pPrDefault><w:pPr><w:spacing w:after="60" w:line="300" w:lineRule="auto"/>'
            '</w:pPr></w:pPrDefault></w:docDefaults>'
            '<w:style w:type="paragraph" w:default="1" w:styleId="Normal">'
            '<w:name w:val="Normal"/><w:qFormat/></w:style>'
            '</w:styles>' % (NS, FONT_EN, FONT_EN, FONT_CN))


def build(name, content):
    """返回 docx 二进制。name 只用于文件名字符串，不进正文（正文自带标题）。"""
    body = md_to_body(content)
    sect = ('<w:sectPr><w:pgSz w:w="11906" w:h="16838"/>'
            '<w:pgMar w:top="1440" w:right="1440" w:bottom="1440" w:left="1440" '
            'w:header="851" w:footer="992" w:gutter="0"/></w:sectPr>')
    doc = ('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
           '<w:document %s><w:body>%s%s</w:body></w:document>' % (NS, body, sect))

    ct = ('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
          '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
          '<Default Extension="rels" '
          'ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
          '<Default Extension="xml" ContentType="application/xml"/>'
          '<Override PartName="/word/document.xml" ContentType="application/vnd.'
          'openxmlformats-officedocument.wordprocessingml.document.main+xml"/>'
          '<Override PartName="/word/styles.xml" ContentType="application/vnd.'
          'openxmlformats-officedocument.wordprocessingml.styles+xml"/>'
          '</Types>')
    rels = ('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
            '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/'
            'officeDocument/2006/relationships/officeDocument" Target="word/document.xml"/>'
            '</Relationships>')
    drels = ('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
             '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
             '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/'
             'officeDocument/2006/relationships/styles" Target="styles.xml"/>'
             '</Relationships>')

    bio = io.BytesIO()
    with zipfile.ZipFile(bio, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("[Content_Types].xml", ct)
        z.writestr("_rels/.rels", rels)
        z.writestr("word/_rels/document.xml.rels", drels)
        z.writestr("word/document.xml", doc)
        z.writestr("word/styles.xml", _styles())
    return bio.getvalue()


def safe_name(name):
    s = re.sub(r'[\\/:*?"<>|\r\n]+', "_", str(name or "报告")).strip() or "报告"
    if not s.lower().endswith(".docx"):
        s += ".docx"
    return s


def api_report_docx(data):
    """POST /api/report_docx -> (blob, filename)。出错抛 ValueError，由调用方转 JSON。"""
    content = (data or {}).get("content") or ""
    name = (data or {}).get("name") or "投研简报"
    if not content.strip():
        raise ValueError("缺少导出内容（content 为空）")
    blob = build(name, content)
    return blob, safe_name("%s_%s" % (name, time.strftime("%Y%m%d")))


if __name__ == "__main__":
    demo = """# 右侧成熟度总榜

> 交易日 2026-11-04 · 数据来源 新浪快照 + 本地日K · 导出时间 10:30:00

## 一、总览

| 指标 | 数值 |
| --- | --- |
| 全市场参与计算 | 5218 只 |
| 位置3 安全位 | 23 只 |
| 位置2 中间位 | 32 只 |

## 二、名单（前 20）

| 排名 | 代码 | 名称 | 综合 | 成熟度 |
| --- | --- | --- | --- | --- |
| 1 | 603273 | 天元智能 | 72 | 61.0 |
| 2 | 600501 | 航天晨光 | 70 | 69.2 |

- 只做位置3，追高位（位置1）一律不碰
- 单笔风险不超过总资金 2%
"""
    blob, fn = api_report_docx({"name": "自测", "content": demo})
    open(fn, "wb").write(blob)
    print("写出", fn, len(blob), "bytes")
