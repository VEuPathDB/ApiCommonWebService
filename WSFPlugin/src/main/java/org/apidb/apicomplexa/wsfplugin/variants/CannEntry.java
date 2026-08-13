package org.apidb.apicomplexa.wsfplugin.variants;

/**
 * One entry of the VCF INFO CANN field, whose format is
 * key|codon|aa|effect|transcript_id|pos_in_cds|pos_in_codon|hgvs_c|hgvs_p
 *
 * r-prefixed keys describe a reference allele per transcript, k-prefixed an alt allele.
 * hgvsC/hgvsP are "." for anything but a coding substitution.
 */
public record CannEntry(
    String key,
    String codon,
    String aminoAcid,
    String effect,
    String transcriptId,
    String posInCds,
    String posInCodon,
    String hgvsC,
    String hgvsP
) {}
