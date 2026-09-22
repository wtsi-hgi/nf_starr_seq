create_html_render <- function(pipeline_name,
                               pipeline_version,
                               lib_type,
                               file_basic_stats,
                               plots_basic_stats,
                               out_render_context)
{
    pipeline_info <- paste0(pipeline_name, " v", pipeline_version)
    rmd_render_context <- glue(r"(
---
title: "STARR-seq data QC Report"
subtitle: "{pipeline_info}"
date: "`r format(Sys.time(), '%d %B %Y -- %A -- %X')`"
output:
    html_document:
        toc: true
        toc_depth: 4
        theme: united
        highlight: tango
---

```{{r setup, include = FALSE}}
knitr::opts_chunk$set(echo = TRUE, fig.align = "center")
library(reactable)
library(sparkline)
library(UpSetR)
```

```{{js, echo = FALSE}}
function formatNumber(num, precision = 1) {{
    const map = [
        {{ suffix: 'T', threshold: 1e12 }},
        {{ suffix: 'B', threshold: 1e9 }},
        {{ suffix: 'M', threshold: 1e6 }},
        {{ suffix: 'K', threshold: 1e3 }},
        {{ suffix: '', threshold: 1 }},
    ];
    const found = map.find((x) => Math.abs(num) >= x.threshold);
    if (found) {{
        const formatted = (num / found.threshold).toFixed(precision) + found.suffix;
        return formatted;
    }}
    return num;
}}

function rangeMore(column, state) {{
    let min = Infinity
    let max = 0
    state.data.forEach(function(row) {{
        const value = row[column.id]
        if (value < min) {{
            min = Math.floor(value)
        }}
        if (value > max) {{
            max = Math.ceil(value)
        }}
    }})

    const filterValue = column.filterValue || min
    const input = React.createElement('input', {{
        type: 'range',
        value: filterValue,
        min: min,
        max: max,
        onChange: function(event) {{
            column.setFilter(event.target.value || undefined)
        }},
        style: {{ width: '100%', marginRight: '8px' }},
        'aria-label': 'Filter ' + column.name
    }})

    return React.createElement(
        'div',
        {{ style: {{ display: 'flex', alignItems: 'center', height: '100%' }} }},
        [input, formatNumber(filterValue)]
    )
}}

function filterMinValue(rows, columnId, filterValue) {{
    return rows.filter(function(row) {{
        return row.values[columnId] >= filterValue
    }})
}}

function rangeLess(column, state) {{
    let min = Infinity
    let max = 0
    state.data.forEach(function(row) {{
        const value = row[column.id]
        if (value < min) {{
            min = Math.floor(value)
        }}
        if (value > max) {{
            max = Math.ceil(value)
        }}
    }})

    const filterValue = column.filterValue || max
    const input = React.createElement('input', {{
        type: 'range',
        value: filterValue,
        min: min,
        max: max,
        onChange: function(event) {{
            column.setFilter(event.target.value || undefined)
        }},
        style: {{ width: '100%', marginRight: '8px' }},
        'aria-label': 'Filter ' + column.name
    }})

    return React.createElement(
        'div',
        {{ style: {{ display: 'flex', alignItems: 'center', height: '100%' }} }},
        [input, formatNumber(filterValue)]
    )
}}

function filterMaxValue(rows, columnId, filterValue) {{
    return rows.filter(function(row) {{
        return row.values[columnId] <= filterValue
    }})
}}
```

---

## 1. Introduction
**Pipeline:** {pipeline_name}

**Version:** {pipeline_version}

**Homepage:** https://github.com/wtsi-hgi/nf_starr_seq

This pipeline is designed for STARR-seq data QC analysis

**Library Type:** {lib_type}
---

## 2. Read Processing
This section summarises the distribution of reads according to the alignments.

* **Num_Raw_Reads:** the number of raw sequencing reads.
* **Num_QCed_Reads:** the number of reads after QC, like adapter trimming, fastqc
* **Num_Align_Reads:** the number of reads aligned to hs1 genome.
* **Num_Dedup_Reads:** the number of aligned reads after deduplication.

```{{r, echo = FALSE}}
df <- as.data.frame(read.table("{file_basic_stats}", header = TRUE, sep = "\t", check.names = FALSE))

df_counts <- df[, c("Sample", "Replicate", grep("^Num_", names(df), value = TRUE))]
min_row <- ifelse(nrow(df) > 10, 10, nrow(df))
reactable(df_counts, 
          highlight = TRUE, 
          bordered = TRUE, 
          striped = TRUE, 
          compact = TRUE, 
          wrap = TRUE,
          minRows = min_row, 
          defaultColDef = colDef(minWidth = 150, align = "left"))

df_pct <- df[, c("Sample", "Replicate", grep("^Pct_", names(df), value = TRUE))]
reactable(df_pct, 
          highlight = TRUE, 
          bordered = TRUE, 
          striped = TRUE, 
          compact = TRUE, 
          wrap = TRUE,
          minRows = min_row, 
          defaultColDef = colDef(minWidth = 150, align = "left"))
```
<br>

```{{r, echo = FALSE, fig.show = "hold", fig.align = "center", out.height = "50%", out.width = "50%"}}
knitr::include_graphics("{plots_basic_stats[1]}", rel_path = FALSE)
```
<br>

```{{r, echo = FALSE, fig.show = "hold", fig.align = "center", out.height = "50%", out.width = "50%"}}
knitr::include_graphics("{plots_basic_stats[2]}", rel_path = FALSE)
```
<br>

    )")

    writeLines(rmd_render_context, out_render_context)
}