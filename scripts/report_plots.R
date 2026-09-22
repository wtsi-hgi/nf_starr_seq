create_venn_diagrams <- function(canonical_barcodes, novel_barcodes, barcode_association)
{
    list_plots <- list()
    for(i in seq_along(canonical_barcodes))
    {
        venn_list <- list(unique(canonical_barcodes[[i]]$barcode), unique(novel_barcodes[[i]]$barcode), barcode_association$barcode)
        names(venn_list) <- c("canonical", "novel", "association")
        p <- ggVennDiagram(venn_list, label_alpha = 0, edge_size = 0.2) + scale_fill_gradient(low = "ivory", high = "tomato")
        list_plots[[i]] <- p
    }
    return(list_plots)
}
