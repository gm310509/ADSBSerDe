/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.teradata.adsbserde;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * A Hive SerDe (Serialiser / Deserialiser) for raw ADSB Data.
 * <p>
 * <pre>
 * The structure of the raw ADSB data is as follows (without the spaces):
 * Key \t value [ \t key \t value ...]
 * An example might be:
 *     clock\t124234\thexid\t\A1B13\talt\t30000
 * The above record contains three fields as follows:
 * </pre>
 * <ul>
 *   <li>clock    124234</li>
 *   <li>hexid    A1B13</li>
 *   <li>alt      30000</li>
 * </ul>
 * </p>
 * <p>
 * This SerDe uses the names of the columns in the table definition to identify
 * the attributes to extract from the ADSB data file.
 * Thus a table defined as follows:
 * <pre>
 *    create table x (
 *       clock    timestamp,
 *       hexid    string,
 *       speed    int
 *    ) ...
 * </pre>
 * Would return the clock (124234), hexid (A1B13) and null for the speed (as
 * there was no speed key in the raw data set. The alt in the raw data set
 * will be ignored because it is not in the table definition.
 * </p>
 * @author Glenn McCall
 *
 */
public class AdsbSerDe extends AbstractSerDe {

    /**
     * The row type information.
     */
    private StructTypeInfo rowTypeInfo;
    
    /**
     * The row Object Inspector.
     */
    private ObjectInspector rowOI;
    
    /**
     * The column names.
     */
    private List<String> colNames;
    
    /**
     * The rows.
     */
    private List<Object> row = new ArrayList<Object>();
    
    private static final String PROP_DEBUG_ENABLED = "debugEnabled";
       
    @Override
    public void initialize(Configuration c, Properties tbl) throws SerDeException {
        MyLogger.println("AsdbSerDe called ... ");
        MyLogger.println("c: " + c.toString());
        MyLogger.println("tbl: " + tbl.toString());

        System.out.println(PROP_DEBUG_ENABLED + "=" + tbl.getProperty(PROP_DEBUG_ENABLED));
        MyLogger.enabled = "true".equalsIgnoreCase(tbl.getProperty(PROP_DEBUG_ENABLED));

        // Generate the regular expression used to extract the ADSB data pairs.
        // This is being done here in the anticipation that one day, parts of this
        // e.g. the separator character - might be accepted as a property of the
        // table definition.
        pairs = Pattern.compile("([\\S^]+)\\s+([\\S$]+)");
        
        String colNamesStr = tbl.getProperty(serdeConstants.LIST_COLUMNS);
        List<String> colNamesWrk = Arrays.asList(colNamesStr.split(","));
        colNames = new LinkedList();
        for (String name : colNamesWrk) {
            if (name != null) {
                colNames.add(name.toLowerCase());
            } else {
                colNames.add(name);
            }
        }
        
        MyLogger.println("colNames: " + colNamesStr);
        
        // Get a list of TypeInfos for the columns. This list lines up with 
        // the list of column names.
        String colTypesStr = tbl.getProperty(serdeConstants.LIST_COLUMN_TYPES);
        List<TypeInfo> colTypes = TypeInfoUtils.getTypeInfosFromTypeString(colTypesStr);
        MyLogger.println("colTypes: " + colTypesStr);

        rowTypeInfo = (StructTypeInfo) TypeInfoFactory.getStructTypeInfo(colNames, colTypes);
        MyLogger.println("rowTypeInfo: " + rowTypeInfo);
        MyLogger.println("clock rowTypeInfo: " + rowTypeInfo.getStructFieldTypeInfo("clock"));
        MyLogger.println("speed rowTypeInfo: " + rowTypeInfo.getStructFieldTypeInfo("speed"));
        
        rowOI = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(rowTypeInfo);
        MyLogger.println("rowOI: " + rowOI);
        
    }

    @Override
    public Class<? extends Writable> getSerializedClass() {
        return Text.class;
    }

    @Override
    public Writable serialize(Object o, ObjectInspector oi) throws SerDeException {
        throw new UnsupportedOperationException("The ADS-B SerDe does not support writes."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public SerDeStats getSerDeStats() {
        return null;
    }

    @Override
    public Object deserialize(Writable wrtbl) throws SerDeException {
        return processRecord(wrtbl.toString(), colNames);
    }

    @Override
    public ObjectInspector getObjectInspector() throws SerDeException {
        return rowOI;
    }
    
    
    private Pattern pairs;
    
    public List<Object> processRecord(String record, List<String> colNames) {
        HashMap<String,String> valueMap = new HashMap();
        
        Matcher m = pairs.matcher(record);
        while(m.find()) {
            String key = m.group(1);
            String value = m.group(2);
//            MyLogger.println("Key:" + key + ", value:" + value);
            valueMap.put(key.toLowerCase(), value);
        }
        
        List<Object> workingRow = new LinkedList<>();
        for (String colName : colNames) {
            String value = valueMap.get(colName);
            Object retValue = value;
            
            TypeInfo typeInfo = rowTypeInfo.getStructFieldTypeInfo(colName);
            if (value != null) {
                try {
                    if ("int".equalsIgnoreCase(typeInfo.getTypeName())) {
                        retValue = Integer.parseInt(value);
                    } else if ("float".equalsIgnoreCase(typeInfo.getTypeName())) {
                        retValue = Float.parseFloat(value);
                    } else if ("timestamp".equalsIgnoreCase(typeInfo.getTypeName())) {
                        long longVal = Long.parseLong(value);
                        retValue = new Timestamp(longVal * 1000);
                        //MyLogger.println("timestamp: " + value + ": " + longVal + ": " + retValue.toString());
                    }
                } catch (Exception e) {
                    MyLogger.println("Caught an exception: " + e.toString() + "\nColumn: " + colName + ", value: " + value);
                    retValue = null;
                }
            }
            workingRow.add(retValue);
        }
        
        return workingRow;
    }
}
