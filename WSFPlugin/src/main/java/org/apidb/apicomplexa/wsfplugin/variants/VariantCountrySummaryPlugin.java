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
 * One row per country. Empty - not an error - when the organism's dnaseq study carries no
 * country attribute; 14 of the 62 dnaseq studies do not, all of them lab lines or
 * reference assemblies.
 */
public class VariantCountrySummaryPlugin extends AbstractPlugin {

  private static final Logger LOG = Logger.getLogger(VariantCountrySummaryPlugin.class);

  static final String[] DATA_COLUMNS =
      { "country", "strain_count", "major_allele", "minor_allele", "other_allele" };

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
        LOG.info("No merged VCF at " + vcf + "; returning an empty country summary.");
        return 0;
      }

      Map<String, String> countries = new SampleMetadataLookup(wdkModel)
          .valuesBySample(locus.edaSuffix(), SampleMetadataLookup.COUNTRY_LABEL);
      if (countries.isEmpty()) {
        LOG.info("Study " + locus.edaSuffix() + " has no country attribute; empty summary.");
        return 0;
      }

      try (MergedVcfReader reader = new MergedVcfReader(vcf)) {
        Optional<LocusCalls> calls = reader.read(locus.sequenceId(), locus.position());
        if (calls.isEmpty()) {
          LOG.warn("Variant " + sourceId + " is absent from " + vcf);
          return 0;
        }

        List<CountryRow> rows = new VariantLocusComposer().countryRows(calls.get(), countries);
        for (CountryRow r : rows) {
          response.addRow(ArrayUtil.concatenate(pkValues, new String[] {
              r.country(), String.valueOf(r.strainCount()),
              r.majorAlleleWithFrequency(), r.minorAlleleWithFrequency(),
              r.otherAlleleWithFrequency()
          }));
        }
      }
    }
    catch (WdkModelException e) {
      throw new PluginModelException("Could not build the country summary for " + sourceId, e);
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
