import { DataFrame } from "@grafana/data";
import { Table } from "apache-arrow";
export declare function marshalArrow(frame: DataFrame): Table;
/**
 * @param keepOriginalNames by default, the exported Table will get names that match the
 * display within grafana.  This typically includes any labels defined in the metadata.
 *
 * When using this function to round-trip data, be sure to set `keepOriginalNames=true`
 */
