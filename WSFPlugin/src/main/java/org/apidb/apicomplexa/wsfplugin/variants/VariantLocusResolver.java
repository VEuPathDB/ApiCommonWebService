package org.apidb.apicomplexa.wsfplugin.variants;

import org.gusdb.wdk.model.WdkModel;
import org.gusdb.wdk.model.WdkModelException;

import javax.sql.DataSource;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;

/**
 * Resolves a Variant primary key to everything the VCF read needs: the tabix
 * coordinate, the organism's file-name form, and the EDA sample table suffix.
 *
 * The VCF path is derived from WdkModel properties rather than a WDK param. The HSSS
 * plugins take a WebServicesPath param whose internal value embeds PROJECT_GOES_HERE,
 * but that exists only because a search has a param form to hang it on; a record table
 * does not.
 */
public class VariantLocusResolver {

  public record Locus(String sequenceId, int position, String nameForFiles, String edaSuffix) {}

  private static final String SQL =
      "SELECT va.sequence_source_id, va.location, o.name_for_filenames, " +
      "       (SELECT DISTINCT s.internal_abbrev || '_' || lower(e.internal_abbrev) " +
      "          FROM apidb.datasource ds " +
      "          JOIN sres.externaldatabase ed ON ed.name = ds.name " +
      "          JOIN sres.externaldatabaserelease edr ON edr.external_database_id = ed.external_database_id " +
      "          JOIN eda.studyexternaldatabaserelease sedr ON sedr.external_database_release_id = edr.external_database_release_id " +
      "          JOIN eda.study s ON s.study_id = sedr.study_id " +
      "          JOIN eda.entitytypegraph e ON e.study_id = s.study_id " +
      "         WHERE ds.type = 'isolates' AND ds.subtype = 'Dna_Seq' " +
      "           AND ds.taxon_id = o.taxon_id AND s.internal_abbrev IS NOT NULL) AS eda_suffix " +
      "FROM ApidbTuning.VariationAttributes va " +
      "JOIN sres.taxonname tn ON tn.name = va.organism AND tn.name_class = 'scientific name' " +
      "JOIN apidb.organism o  ON o.taxon_id = tn.taxon_id " +
      "WHERE va.source_id = ?";

  private final WdkModel _wdkModel;

  public VariantLocusResolver(WdkModel wdkModel) {
    _wdkModel = wdkModel;
  }

  public Optional<Locus> resolve(String sourceId) throws WdkModelException {
    DataSource ds = _wdkModel.getAppDb().getDataSource();
    try (Connection conn = ds.getConnection();
         PreparedStatement ps = conn.prepareStatement(SQL)) {
      ps.setString(1, sourceId);
      try (ResultSet rs = ps.executeQuery()) {
        if (!rs.next()) return Optional.empty();
        return Optional.of(new Locus(
            rs.getString(1), rs.getInt(2), rs.getString(3), rs.getString(4)));
      }
    }
    catch (SQLException e) {
      throw new WdkModelException("Could not resolve variant locus for " + sourceId, e);
    }
  }

  /**
   * ${WEBSERVICEMIRROR}/${projectId}/build-${buildNumber}/${nameForFiles}/dnaseq/vcf/merged.ann.vcf.gz
   * — the same three properties JBrowseService uses.
   */
  public Path vcfPath(Locus locus) throws WdkModelException {
    String mirror = _wdkModel.getProperties().get("WEBSERVICEMIRROR");
    if (mirror == null || mirror.isBlank()) {
      throw new WdkModelException("WdkModel property WEBSERVICEMIRROR is not set");
    }
    String buildNumber = _wdkModel.getBuildNumber();
    if (buildNumber == null || buildNumber.isBlank()) {
      throw new WdkModelException("WdkModel build number is not set");
    }
    return Paths.get(mirror,
        _wdkModel.getProjectId(),
        "build-" + buildNumber,
        locus.nameForFiles(),
        "dnaseq", "vcf", "merged.ann.vcf.gz");
  }
}
