package org.apidb.apicomplexa.wsfplugin.variants;

import org.apache.log4j.Logger;
import org.gusdb.fgputil.ArrayUtil;
import org.gusdb.fgputil.json.JsonUtil;
import org.gusdb.fgputil.runtime.InstanceManager;
import org.gusdb.wdk.model.Utilities;
import org.gusdb.wdk.model.WdkModel;
import org.gusdb.wdk.model.WdkModelException;
import org.gusdb.wsf.plugin.AbstractPlugin;
import org.gusdb.wsf.plugin.DelayedResultException;
import org.gusdb.wsf.plugin.PluginModelException;
import org.gusdb.wsf.plugin.PluginRequest;
import org.gusdb.wsf.plugin.PluginResponse;
import org.gusdb.wsf.plugin.PluginUserException;
import org.json.JSONArray;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.gusdb.wdk.model.answer.single.SingleRecordQuestionParam.PRIMARY_KEY_PARAM_NAME;
import static org.gusdb.wdk.model.record.TableField.TABLE_NAME_PARAM_NAME;

/**
 * The shared control flow behind every Variant record-page table backed by the merged
 * VCF: decode the primary key, resolve the locus, find the VCF, load the study's EDA
 * sample metadata, read the locus, and emit one row per composed cell array.
 *
 * The flow is here rather than in each plugin because it carries invariants that are
 * easy to break by copy: which conditions are a benign empty table (and at which log
 * level), that the primary-key columns always prefix the data columns in the same
 * order, and that a WDK/htsjdk failure becomes a loud {@link PluginModelException}
 * naming the variant rather than a silently empty table.
 *
 * A subclass supplies only its column names, a label for messages, and the cell
 * composition; it must not need to touch the flow.
 */
public abstract class AbstractVariantLocusTablePlugin extends AbstractPlugin {

  private static final Logger LOG = Logger.getLogger(AbstractVariantLocusTablePlugin.class);

  /** This table's own columns, appended after the record's primary-key columns. */
  protected abstract String[] dataColumns();

  /** Names this table in log and exception messages, e.g. "strains table". */
  protected abstract String tableLabel();

  /**
   * The rows, as arrays of cell values in {@link #dataColumns()} order. A transposition
   * here is silent and wrong on the page, so the two must be read together.
   */
  protected abstract List<String[]> dataCells(LocusCalls calls, Map<String, String> countryBySample);

  /**
   * True when an empty sample-metadata map means there is nothing to show, so the VCF
   * need not be opened at all.
   */
  protected boolean requiresSampleMetadata() {
    return false;
  }

  @Override
  public String[] getRequiredParameterNames() {
    return new String[] { PRIMARY_KEY_PARAM_NAME, TABLE_NAME_PARAM_NAME };
  }

  @Override
  public String[] getColumns(PluginRequest request) throws PluginModelException {
    return ArrayUtil.concatenate(getPkColumnNames(request), dataColumns());
  }

  @Override
  public void validateParameters(PluginRequest request) {
    // PK value has already been validated by the record service.
  }

  @Override
  protected int execute(PluginRequest request, PluginResponse response)
      throws PluginModelException, PluginUserException, DelayedResultException {

    String[] pkValues = JsonUtil.toStringArray(
        new JSONArray(request.getParams().get(PRIMARY_KEY_PARAM_NAME)));
    String sourceId = pkValues[0];

    WdkModel wdkModel = InstanceManager.getInstance(WdkModel.class, request.getProjectId());

    Path vcf = null;
    try {
      VariantLocusResolver resolver = new VariantLocusResolver(wdkModel);
      Optional<VariantLocusResolver.Locus> maybeLocus = resolver.resolve(sourceId);
      if (maybeLocus.isEmpty()) {
        LOG.warn("No locus row for variant " + sourceId + "; returning an empty table.");
        return 0;
      }
      VariantLocusResolver.Locus locus = maybeLocus.get();

      vcf = resolver.vcfPath(locus);
      if (!Files.exists(vcf)) {
        // Not every organism has a merged call set. Absence is normal and silent,
        // matching JbrowseOrgSpecificNaTracks.pm.
        LOG.info("No merged VCF at " + vcf + "; returning an empty " + tableLabel() + ".");
        return 0;
      }

      Map<String, String> countries = new SampleMetadataLookup(wdkModel)
          .valuesBySample(locus.edaSuffix(), SampleMetadataLookup.COUNTRY_LABEL);
      if (requiresSampleMetadata() && countries.isEmpty()) {
        // Deliberately before the reader is constructed: a table keyed on country has
        // nothing to show without the metadata, so opening (and tabix-querying) the VCF
        // would be pure cost. Covers all three ways the map comes back empty - no dnaseq
        // study for the organism (null suffix), the eda tables absent, or a study that
        // genuinely carries no country attribute - none of which is an error.
        LOG.info("No country metadata for study " + locus.edaSuffix() + " (variant "
            + sourceId + "); empty " + tableLabel() + ".");
        return 0;
      }

      try (MergedVcfReader reader = new MergedVcfReader(vcf)) {
        Optional<LocusCalls> calls = reader.read(locus.sequenceId(), locus.position());
        if (calls.isEmpty()) {
          LOG.warn("Variant " + sourceId + " is absent from " + vcf);
          return 0;
        }
        for (String[] cells : dataCells(calls.get(), countries)) {
          response.addRow(ArrayUtil.concatenate(pkValues, cells));
        }
      }
    }
    catch (WdkModelException e) {
      throw new PluginModelException(
          "Could not build the " + tableLabel() + " for " + sourceId, e);
    }
    catch (htsjdk.tribble.TribbleException | java.io.UncheckedIOException e) {
      // Files.exists(vcf) proves nothing about the .tbi, and MergedVcfReader opens with
      // requireIndex = true. A missing or stale index, a truncated bgzf block, or an IO
      // error mid-query throws unchecked past the WdkModelException catch above, losing
      // both the variant and the file that failed. Stays an error: an empty table here
      // would misreport a broken mirror as "this variant has no samples".
      throw new PluginModelException("Could not read the merged VCF " + vcf
          + " for the " + tableLabel() + " of " + sourceId, e);
    }
    return 0;
  }

  private String[] getPkColumnNames(PluginRequest request) {
    WdkModel wdkModel = InstanceManager.getInstance(WdkModel.class, request.getProjectId());
    String questionFullName = request.getContext().get(Utilities.CONTEXT_KEY_QUESTION_FULL_NAME);
    return wdkModel.getQuestionByFullName(questionFullName).orElseThrow()
        .getRecordClass().getPrimaryKeyDefinition().getColumnRefs();
  }
}
