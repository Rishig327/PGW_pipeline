import apache_beam as beam
 

def build_key_with_filename(dimensions):
    
    def _build_key_with_filename(row):
        """
        Builds tuple key using configured dimensions.
        """
        key = tuple(row.get(dim, "") for dim in dimensions)

        key = key + (row.get("filename", ""),)
    
        return key, row
    return _build_key_with_filename

def build_key(dimensions):
    
    def _build_key(row):
        """
        Builds tuple key using configured dimensions.
        """
        key = tuple(row.get(dim, "") for dim in dimensions)

        return key, row
    return _build_key



def format_agg_output(element):
    key, measures = element
    
    *dimensions, filename = key   # filename is last in key
 
    return {
        "filename": filename,
        "dimensions": dimensions,
        "measures": measures
    }

def format_agg_output_for_bq_fn(agg_dimensions):
        
    def _format_agg_output_for_bq(element):

        key, measures = element
        dimensions = key

        result_agg = {}

        # Map dimensions dynamically by name
        for i, dim_name in enumerate(agg_dimensions):
            result_agg[dim_name] = str(dimensions[i]) if i < len(dimensions) and dimensions[i] is not None else ""

        # Add measures dynamically (keep measure field names as-is)
        if measures:
            for m_name, m_val in measures.items():
                result_agg[m_name] = m_val

        return result_agg
    return _format_agg_output_for_bq

class TelecomAggregateFn(beam.CombineFn):
    
    def __init__(self, agg_measures: dict):
        self.agg_measures = agg_measures

    # ---------------------------
    # Create accumulator
    # ---------------------------
    def create_accumulator(self):
        acc = {}
 
        for out_field, rule in self.agg_measures .items():
            agg_type = rule["agg"]
 
            if agg_type in ("sum", "count"):
                acc[out_field] = 0
 
            elif agg_type == "min":
                acc[out_field] = float("inf")
 
            elif agg_type == "max":
                acc[out_field] = float("-inf")
 
            elif agg_type == "avg":
                acc[out_field] = {"sum": 0, "count": 0}
        
        return acc
 
 
    # ---------------------------
    # Add input record
    # ---------------------------
    def add_input(self, acc, row):
 
        for out_field, rule in self.agg_measures .items():
 
            agg_type = rule["agg"]
            source_field = rule.get("field")
 
            if agg_type == "sum":
                acc[out_field] += float(row.get(source_field, 0))
 
            elif agg_type == "count":
                acc[out_field] += 1
 
            elif agg_type == "min":
                val = float(row.get(source_field, float("inf")))
                acc[out_field] = min(acc[out_field], val)
 
            elif agg_type == "max":
                val = float(row.get(source_field, float("-inf")))
                acc[out_field] = max(acc[out_field], val)
 
            elif agg_type == "avg":
                val = float(row.get(source_field, 0))
                acc[out_field]["sum"] += val
                acc[out_field]["count"] += 1
        
        return acc
 
 
    # ---------------------------
    # Merge accumulators (Distributed Safe)
    # ---------------------------
    def merge_accumulators(self, accumulators):
 
        merged = self.create_accumulator()
 
        for acc in accumulators:
            for field, rule in self.agg_measures .items():
 
                agg_type = rule["agg"]
 
                if agg_type in ("sum", "count"):
                    merged[field] += acc[field]
 
                elif agg_type == "min":
                    merged[field] = min(merged[field], acc[field])
 
                elif agg_type == "max":
                    merged[field] = max(merged[field], acc[field])
 
                elif agg_type == "avg":
                    merged[field]["sum"] += acc[field]["sum"]
                    merged[field]["count"] += acc[field]["count"]
 
        return merged
 
 
    # ---------------------------
    # Final Output
    # ---------------------------
    def extract_output(self, acc):
 
        result = {}
 
        for field, rule in self.agg_measures .items():
 
            agg_type = rule["agg"]
 
            if agg_type == "avg":
                total = acc[field]["sum"]
                count = acc[field]["count"]
                result[field] = round(total / count, 2) if count else 0
 
            else:
                result[field] = acc[field]
 
        return result