package org.apidb.apicomplexa.wsfplugin.variants;

import java.util.List;

public record LocusCalls(
    String sequenceId,
    int position,
    String refAllele,
    List<String> altAlleles,
    List<SampleCall> calls
) {}
