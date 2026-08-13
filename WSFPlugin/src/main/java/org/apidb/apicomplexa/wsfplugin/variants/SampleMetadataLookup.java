package org.apidb.apicomplexa.wsfplugin.variants;

import org.gusdb.wdk.model.WdkModel;
import org.gusdb.wdk.model.WdkModelException;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * EDA sample metadata for one dnaseq study.
 *
 * VCF sample names ARE EDA sample_stable_ids, so no mapping layer is needed - established
 * in 2026-08-05-hsss-variation-plumbing-design.md section 6.
 *
 * Keyed on provider_label rather than stable_id: EDA stable_ids are VAR_&lt;hash&gt; digests
 * of the provider label, stable per label but site-specific.
 *
 * Successful lookups are memoized process-wide for the life of the webapp - see
 * {@link #CACHE}.
 */
public class SampleMetadataLookup {

  /** Verified: covers 536 of 537 Pf samples, against geographic_location's 111. */
  public static final String COUNTRY_LABEL = "[\"country\"]";

  private record CacheKey(String edaSuffix, String providerLabel) {}

  /**
   * (edaSuffix, providerLabel) -&gt; that study's values, held for the life of the webapp.
   *
   * Why caching is safe: the query is keyed on the STUDY and the attribute, not on the
   * variant, so a variant record page re-runs the identical ~537-row join on every view
   * - twice per page once both variant tables are present - and record-page tables are
   * uncached by WDK. There are only ~62 dnaseq studies, and eda.attributevalue_* is
   * written by a data load rather than by the site, so the values are static between
   * builds; a build reloads the webapp, which discards this map. That reload is the
   * invalidation, so no explicit eviction exists.
   *
   * ConcurrentHashMap because concurrent page loads hit this. Entries are immutable
   * copies, so a caller cannot mutate the shared value. A genuinely empty result IS
   * cached: a study with no country attribute is a stable fact about the study (14 of
   * the 62), not a failure. The 42P01 path deliberately is NOT cached - see
   * {@link #valuesBySample}.
   */
  private static final Map<CacheKey, Map<String, String>> CACHE = new ConcurrentHashMap<>();

  private static final String SQL_TEMPLATE =
      "SELECT av.sample_stable_id, av.string_value " +
      "FROM eda.attributevalue_%1$s av " +
      "JOIN eda.attributegraph_%1$s ag ON ag.stable_id = av.attribute_stable_id " +
      "WHERE ag.provider_label = ? AND av.string_value IS NOT NULL";

  private final WdkModel _wdkModel;

  public SampleMetadataLookup(WdkModel wdkModel) {
    _wdkModel = wdkModel;
  }

  /**
   * sample_stable_id -&gt; value for one attribute. Returns an empty map when the study
   * has no such attribute at all, which is normal: 14 of the 62 dnaseq studies carry no
   * country attribute (lab lines and reference assemblies).
   *
   * Values are trimmed, and a sample whose value is blank after trimming is omitted
   * from the map entirely, so every value present is non-empty. Callers may therefore
   * treat "absent from the map" as the single representation of "no value".
   *
   * The returned map is immutable, and a successful lookup is memoized in {@link #CACHE}
   * for the life of the webapp.
   */
  public Map<String, String> valuesBySample(String edaSuffix, String providerLabel)
      throws WdkModelException {
    // VariantLocusResolver's subquery returns NULL for edaSuffix when the organism has
    // no dnaseq EDA study at all. That is a legitimate state - no study means no sample
    // metadata - so it yields an empty map, not an exception.
    if (edaSuffix == null || edaSuffix.isBlank()) {
      return Map.of();
    }

    // edaSuffix is not user input - it comes from apidb/eda catalogue tables via
    // VariantLocusResolver - but validate anyway, since it is interpolated as an
    // identifier. A non-null value that fails this is a genuine invariant violation,
    // unlike the null case above, so it stays a hard failure.
    if (!edaSuffix.matches("[A-Za-z0-9_]+")) {
      throw new WdkModelException("Refusing to use unsafe EDA table suffix: " + edaSuffix);
    }

    CacheKey key = new CacheKey(edaSuffix, providerLabel);
    Map<String, String> memo = CACHE.get(key);
    if (memo != null) return memo;

    Map<String, String> out = new LinkedHashMap<>();
    DataSource ds = _wdkModel.getAppDb().getDataSource();
    String sql = String.format(SQL_TEMPLATE, edaSuffix);

    try (Connection conn = ds.getConnection();
         PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setString(1, providerLabel);
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          // Normalised at the boundary, not at each consumer: EDA string_values carry
          // stray whitespace, and an untrimmed "Mali " would aggregate as a second,
          // distinct country. A value that is blank once trimmed carries no information,
          // so it is absent from the map rather than present as "" - callers already
          // treat "missing" as the empty string.
          String value = rs.getString(2);
          if (value == null) continue;
          value = value.trim();
          if (value.isEmpty()) continue;
          out.put(rs.getString(1), value);
        }
      }
    }
    catch (SQLException e) {
      if (isUndefinedTable(e)) {
        // Postgres SQLState 42P01 ("undefined_table"): attributevalue_<suffix> /
        // attributegraph_<suffix> don't exist, which means this organism has no dnaseq
        // EDA study - an empty map is the correct answer, not a page error. Any other
        // SQLState (connection refusal, pool exhaustion, timeout, ...) is a real fault
        // and must not be mistaken for that benign case.
        //
        // Deliberately NOT memoized, unlike a successful empty result: this is an
        // environmental fact, not a fact about the study. The eda tables can appear
        // from a data load without the webapp restarting, and a cached empty would pin
        // the site in the broken state until someone redeployed it.
        return Map.of();
      }
      throw new WdkModelException(
          "Could not look up sample metadata for EDA suffix " + edaSuffix, e);
    }

    // Immutable copy, so a caller cannot mutate the entry every other page view shares.
    Map<String, String> immutable = Map.copyOf(out);
    CACHE.put(key, immutable);
    return immutable;
  }

  /**
   * Walks both the SQLException chain (getNextException(), which pooled/wrapping
   * drivers use to carry the underlying error) and the general cause chain, looking for
   * SQLState 42P01. Checked by SQLState rather than a vendor error code so this isn't
   * tied to a specific PostgreSQL driver version.
   */
  private static boolean isUndefinedTable(Throwable t) {
    return isUndefinedTable(t, Collections.newSetFromMap(new IdentityHashMap<>()));
  }

  /**
   * The visited set guards against a cyclic exception chain: a self-referencing
   * getCause() is handled by Throwable itself, but a mutual cycle across two exceptions
   * is constructible, and cyclic getNextException() chains are something real pooled
   * JDBC drivers produce. Without this, that shape recurses forever and turns a benign
   * "table missing" check into a StackOverflowError - identity (not equals()) is used
   * because distinct exception instances can compare equal.
   */
  private static boolean isUndefinedTable(Throwable t, Set<Throwable> visited) {
    while (t != null && visited.add(t)) {
      if (t instanceof SQLException se) {
        if ("42P01".equals(se.getSQLState())) return true;
        if (isUndefinedTable(se.getNextException(), visited)) return true;
      }
      t = t.getCause();
    }
    return false;
  }
}
