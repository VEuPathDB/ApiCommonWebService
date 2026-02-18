package org.apidb.apicomplexa.wsfplugin.eda;

import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import javax.ws.rs.core.MediaType;

import org.apache.log4j.Logger;
import org.gusdb.fgputil.IoUtil;
import org.gusdb.fgputil.client.ClientUtil;
import org.gusdb.wsf.plugin.PluginModelException;
import org.json.JSONArray;
import org.json.JSONObject;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

public class GeneEdaVizWithComputePlugin extends AbstractEdaGenesPlugin {

  private static final Logger LOG = Logger.getLogger(GeneEdaVizWithComputePlugin.class);

  private static class Point {

    String pointId;
    String effectSize;
    String pValue;

    @Override
    public String toString() {
      // column order must match dynamic column order defined in the model XML
      //LOG.info("Returning converted row [" + pointId + ", " + effectSize + ", " + pValue + "]");
      return pointId + "\t" + effectSize + "\t" + pValue;
    }
  }

  private double _effectSizeThreshold;
  private double _significanceThreshold;
  private Path _tmpFile;

  @Override
  protected InputStream getEdaTabularDataStream(String edaBaseUrl, Map<String, String> authHeader)
      throws Exception {
    
    /** Expected analysis format
     * {
     *   "displayName":"Volcano plot example",
     *   "description":"",
     *   "descriptor":{
     *     "subset":{
     *       "descriptor":[],
     *       "uiSettings":{}
     *     },
     *     "computations":[{             <- computeJson
     *       "computationId":"9l2qi",
     *       "descriptor":{              <- computeDescriptor
     *         "type":"differentialabundance",  <- computeName
     *         "configuration":{                <- computeConfig
     *           "differentialAbundanceMethod":"Maaslin",
     *           "pValueFloor":"1e-200",
     *           "collectionVariable":{
     *             "entityId":"OBI_0002623",
     *             "collectionId":"EUPATH_0009253"
     *           },
     *           "comparator":{
     *             "variable":{
     *               "entityId":"EUPATH_0000096",
     *               "variableId":"EUPATH_0000639"
     *             },
     *             "groupA":[{
     *               "label":"Control"
     *             }],
     *             "groupB":[{
     *               "label":"Cystic fibrosis"
     *             }]
     *           }
     *         }
     *       },
     *       "visualizations":[{
     *         "visualizationId":"33f58eee-b3fa-4d38-8744-7c01d36d7729",
     *         "displayName":"Volcano Example",
     *         "descriptor":{           <- vizDescriptor
     *           "type":"volcanoplot",  <- vizName
     *           "configuration":{      <- vizConfig
     *             "effectSizeThreshold":1,
     *             "significanceThreshold":0.05,
     *             "markerBodyOpacity":0.8
     *           },
     *           "thumbnail":"data:image/jpeg;base64,/9j/4AAQS2Q==",
     *           "currentPlotFilters":[]
     *         }
     *       }]
     *     }],
     *     "starredVariables":[],
     *     "dataTableConfig":{},
     *     "derivedVariables":[]
     *   },
     *   "isPublic":false
     * }
     */

    // break up analysis spec to find required pieces
    JSONArray filters = _analysisSpec
        .getJSONObject("descriptor")
        .getJSONObject("subset")
        .getJSONArray("descriptor");
    JSONObject computeJson = findVolcanoComputation(
        _analysisSpec.getJSONObject("descriptor").getJSONArray("computations"));
    JSONObject computeDescriptor = computeJson.getJSONObject("descriptor");
    String computeName = computeDescriptor.getString("type");
    JSONObject computeConfig = computeDescriptor.getJSONObject("configuration");

    JSONObject vizDescriptor = computeJson
        .getJSONArray("visualizations").getJSONObject(0)
        .getJSONObject("descriptor");
    String vizName = vizDescriptor.getString("type");
    JSONObject vizConfig = vizDescriptor.getJSONObject("configuration");

    // values to be used later to filter returned rows
    _effectSizeThreshold = vizConfig.getDouble("effectSizeThreshold");
    _significanceThreshold = vizConfig.getDouble("significanceThreshold");

    // make request with JSON like
    /**
     * {
     *   "studyId":"BONUS-1",
     *   "filters":[],
     *   "config":{},
     *   "computeConfig":{
     *     "differentialExpressionMethod":"DESeq",
     *     "pValueFloor":"1e-200",
     *     "collectionVariable":{
     *       "entityId":"OBI_0002623",
     *       "collectionId":"EUPATH_0009253"
     *     },
     *     "comparator":{
     *       "variable":{
     *         "entityId":"EUPATH_0000096",
     *         "variableId":"EUPATH_0009091"
     *       },
     *       "groupA":[{
     *         "label":"Homozygous F508del"
     *       }],
     *       "groupB":[{
     *         "label":"Heterozygous F508del"
     *       }]
     *     }
     *   }
     * }
     */

    String requestUrl = edaBaseUrl + "/apps/" + computeName + "/visualizations/" + vizName;
    JSONObject requestBody = new JSONObject()
        .put("studyId", _studyId)
        .put("filters", filters)
        .put("computeConfig", computeConfig)
        // assume viz settings are client-side only
        .put("config", new JSONObject());

    _tmpFile = Files.createTempFile(_wdkModel.getModelConfig().getWdkTempDir(), "eda-" + computeName + "-" + vizName, ".tab");
    IoUtil.openPosixPermissions(_tmpFile);
    LOG.info("Wrote temp file to contain EDA output as tabular: " + _tmpFile);

    // make request to EDA for volcano plot data, convert JSON response to tabular, and write to temporary file
    try (InputStream in = ClientUtil
           .makeAsyncPostRequest(requestUrl, requestBody, MediaType.APPLICATION_JSON, authHeader)
           .getInputStream();
         BufferedWriter out = new BufferedWriter(new FileWriter(_tmpFile.toFile(), StandardCharsets.UTF_8))) {
      JsonParser parser = new JsonFactory().createParser(in);
      while (parser.nextToken() != JsonToken.END_OBJECT) {
        if ("statistics".equals(parser.currentName())) {
          while (parser.nextToken() != JsonToken.END_ARRAY) {
            Point p = new Point();
            while (parser.nextToken() != JsonToken.END_OBJECT) {
              if (parser.currentName() == null) continue;
              switch(parser.currentName()) {
                case "pointID":
                  parser.nextToken();
                  p.pointId = parser.getText();
                  break;
                case "effectSize":
                  parser.nextToken();
                  p.effectSize = parser.getText();
                  break;
                case "pValue":
                  parser.nextToken();
                  p.pValue = parser.getText();
              }
            }
            out.write(p.toString());
            out.newLine();
          }
        }
      }
    }

    return new BufferedInputStream(new FileInputStream(_tmpFile.toFile()));

    /** should return JSON like:
{
    "effectSizeLabel": "log2(Fold Change)",
    "pValueFloor": "1e-200",
    "adjustedPValueFloor": null,
    "statistics": [
        {
            "effectSize": "0.242298022720962",
            "pValue": "0.828149584648419",
            "adjustedPValue": "0.931437237077219",
            "pointID": "OBI_0002623.EUPATH_0009253_Bacteria_Proteobacteria_Proteobacteria_unclassified"
        },
        {
            "effectSize": "-0.288881229204018",
            "pValue": "0.0432024553023979",
            "adjustedPValue": "0.155528839088632",
            "pointID": "OBI_0002623.EUPATH_0009253_Bacteria_Actinobacteria_Actinobacteria"
        },
        {
            "effectSize": "0.079528991325269",
            "pValue": "0.521147648551248",
            "adjustedPValue": "0.852787061265679",
            "pointID": "OBI_0002623.EUPATH_0009253_Bacteria_Firmicutes_Clostridia"
        },
        {
            "effectSize": "0.460715113778634",
            "pValue": "0.009531423577601",
            "adjustedPValue": "0.0802561106694941",
            "pointID": "OBI_0002623.EUPATH_0009253_Bacteria_Firmicutes_Erysipelotrichia"
        },
        {
            "effectSize": "0.651977429118175",
            "pValue": "0.200742684545636",
            "adjustedPValue": "0.516195474545921",
            "pointID": "OBI_0002623.EUPATH_0009253_Bacteria_Firmicutes_Tissierellia"
        },
        {
            "effectSize": "0.336017744530915",
            "pValue": "0.486681385049771",
            "adjustedPValue": "0.852787061265679",
            "pointID": "OBI_0002623.EUPATH_0009253_Bacteria_Proteobacteria_Betaproteobacteria"
        },
        {
            "effectSize": "-0.17692567121528",
            "pValue": "0.911840684146044",
            "adjustedPValue": "0.931437237077219",
            "pointID": "OBI_0002623.EUPATH_0009253_Bacteria_Proteobacteria_Epsilonproteobacteria"
        },
        {
            "effectSize": "-0.746501138245443",
            "pValue": "0.000330749431205946",
            "adjustedPValue": "0.00595348976170704",
            "pointID": "OBI_0002623.EUPATH_0009253_Bacteria_Actinobacteria_Coriobacteriia"
        },
        {
            "effectSize": "0.184535541916011",
            "pValue": "0.781539088791623",
            "adjustedPValue": "0.931437237077219",
            "pointID": "OBI_0002623.EUPATH_0009253_Bacteria_Fusobacteria_Fusobacteriia"
        },
        {
            "effectSize": "-0.16277387261538",
            "pValue": "0.931437237077219",
            "adjustedPValue": "0.931437237077219",
            "pointID": "OBI_0002623.EUPATH_0009253_Eukaryota_Ascomycota_Saccharomycetes"
        },
        {
            "effectSize": "-0.0907783195527249",
            "pValue": "0.747690142844642",
            "adjustedPValue": "0.931437237077219",
            "pointID": "OBI_0002623.EUPATH_0009253_Bacteria_Bacteroidota_Bacteroidia"
        },
        {
            "effectSize": "-0.536320514426692",
            "pValue": "0.410295507192367",
            "adjustedPValue": "0.820591014384734",
            "pointID": "OBI_0002623.EUPATH_0009253_Bacteria_Firmicutes_Firmicutes_unclassified"
        },
        {
            "effectSize": "-0.269624110082278",
            "pValue": "0.0633990309186218",
            "adjustedPValue": "0.190197092755865",
            "pointID": "OBI_0002623.EUPATH_0009253_Bacteria_Firmicutes_Negativicutes"
        },
        {
            "effectSize": "-0.280956635685237",
            "pValue": "0.0133760184449157",
            "adjustedPValue": "0.0802561106694941",
            "pointID": "OBI_0002623.EUPATH_0009253_Bacteria_Firmicutes_Bacilli"
        },
        {
            "effectSize": "-0.177580905613535",
            "pValue": "0.924648007959414",
            "adjustedPValue": "0.931437237077219",
            "pointID": "OBI_0002623.EUPATH_0009253_Bacteria_Chlamydiae_Chlamydiia"
        },
        {
            "effectSize": "-0.13686598606319",
            "pValue": "0.31776611734392",
            "adjustedPValue": "0.714973764023821",
            "pointID": "OBI_0002623.EUPATH_0009253_Bacteria_Proteobacteria_Gammaproteobacteria"
        },
        {
            "effectSize": "-0.138899347072534",
            "pValue": "0.914140740352587",
            "adjustedPValue": "0.931437237077219",
            "pointID": "OBI_0002623.EUPATH_0009253_Bacteria_Proteobacteria_Deltaproteobacteria"
        },
        {
            "effectSize": "2.35756519537476",
            "pValue": "0.0221519393209508",
            "adjustedPValue": "0.0996837269442787",
            "pointID": "OBI_0002623.EUPATH_0009253_Bacteria_Verrucomicrobia_Verrucomicrobiae"
        }
    ]
}
     */
  }

  @Override
  protected Boolean isRetainedRow(String[] edaRow) {
    try {
      //LOG.info("Checking if edaRow of size " + edaRow.length + " should be retained, array = [ " + String.join(", ", edaRow) + " ]");
      double effectSize = Math.abs(Double.valueOf(edaRow[1]));
      double pValue = Math.abs(Double.valueOf(edaRow[2]));
      return pValue <= _significanceThreshold && effectSize >= _effectSizeThreshold;
    }
    catch (NumberFormatException e) {
      LOG.warn("Skipping EDA output row in which effectSize or pValue property is not a valid double value. Row = [ " + String.join(", ", edaRow) + " ]", e);
      return false;
    }
  }

  @Override
  protected Object[] convertToTmpTableRow(String[] edaRow) {
    //LOG.info("Converting edaRow of size " + edaRow.length + " from temporary file (tabular), array = [ " + String.join(", ", edaRow) + " ]");
    return new Object[] { edaRow[0], edaRow[1], edaRow[2] };
  }

  /**
   * Find the computation containing a volcano plot visualization with
   * the threshold configuration this plugin requires.
   */
  private static JSONObject findVolcanoComputation(JSONArray computations) throws PluginModelException {
    for (int i = 0; i < computations.length(); i++) {
      JSONObject comp = computations.getJSONObject(i);
      JSONArray vizs = comp.optJSONArray("visualizations");
      if (vizs == null) continue;
      for (int j = 0; j < vizs.length(); j++) {
        JSONObject vizDesc = vizs.getJSONObject(j).optJSONObject("descriptor");
        if (vizDesc == null) continue;
        if (!"volcanoplot".equals(vizDesc.optString("type"))) continue;
        JSONObject vizConfig = vizDesc.optJSONObject("configuration");
        if (vizConfig == null) continue;
        if (vizConfig.has("effectSizeThreshold") && vizConfig.has("significanceThreshold")) {
          return comp;
        }
      }
    }
    throw new PluginModelException(
      "Analysis spec does not contain a computation with a volcano plot visualization " +
      "configured with effectSizeThreshold and significanceThreshold.");
  }

}
