package org.apidb.apicomplexa.wsfplugin.variants;

import java.util.List;

public record LocusCalls(
    String sequenceId,
    int position,
    String refAllele,
    List<String> altAlleles,
    CannIndex cann,
    List<SampleCall> calls
) {}
