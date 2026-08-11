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

/** One row per VCF sample at this variant's locus. */
public class VariantStrainsPlugin extends AbstractPlugin {

  private static final Logger LOG = Logger.getLogger(VariantStrainsPlugin.class);

  static final String[] DATA_COLUMNS =
      { "strain", "country", "genotype", "allele", "depth", "read_frequency", "aa_product" };

  @Override
  public String[] getRequiredParameterNames() {
    return new String[] { PRIMARY_KEY_PARAM_NAME, TABLE_NAME_PARAM_NAME };
  }

  @Override
  public String[] getColumns(PluginRequest request) throws PluginModelException {
    return ArrayUtil.concatenate(getPkColumnNames(request), DATA_COLUMNS);
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

    try {
      VariantLocusResolver resolver = new VariantLocusResolver(wdkModel);
      Optional<VariantLocusResolver.Locus> maybeLocus = resolver.resolve(sourceId);
      if (maybeLocus.isEmpty()) {
        LOG.warn("No locus row for variant " + sourceId + "; returning an empty table.");
        return 0;
      }
      VariantLocusResolver.Locus locus = maybeLocus.get();

      Path vcf = resolver.vcfPath(locus);
      if (!Files.exists(vcf)) {
        // Not every organism has a merged call set. Absence is normal and silent,
        // matching JbrowseOrgSpecificNaTracks.pm.
        LOG.info("No merged VCF at " + vcf + "; returning an empty table.");
        return 0;
      }

      Map<String, String> countries = new SampleMetadataLookup(wdkModel)
          .valuesBySample(locus.edaSuffix(), SampleMetadataLookup.COUNTRY_LABEL);

      try (MergedVcfReader reader = new MergedVcfReader(vcf)) {
        Optional<LocusCalls> calls = reader.read(locus.sequenceId(), locus.position());
        if (calls.isEmpty()) {
          LOG.warn("Variant " + sourceId + " is absent from " + vcf);
          return 0;
        }
        List<StrainRow> rows = new VariantLocusComposer().strainRows(calls.get(), countries);
        for (StrainRow r : rows) {
          response.addRow(ArrayUtil.concatenate(pkValues, new String[] {
              r.strain(), r.country(), r.genotype(), r.allele(),
              r.depth(), r.readFrequency(), r.aaProduct()
          }));
        }
      }
    }
    catch (WdkModelException e) {
      throw new PluginModelException("Could not build the strains table for " + sourceId, e);
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
