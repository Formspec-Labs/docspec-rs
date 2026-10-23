//! Custom DataFusion `AggregateUDFImpl` implementations for `FirstValueBy`
//! and `CollectDistinctWithProvenance`.

use crate::engine::write::merge::{
    OrdScalar, coerce_array_to_field, compare_non_null, scalars_to_array,
};
use crate::model::{NullOrdering, SortColumn, SortDirection};
use arrow::{
    array::{Array, ArrayRef, BooleanArray, GenericListArray, StructArray, new_empty_array},
    buffer::OffsetBuffer,
    datatypes::{DataType, Field, FieldRef, Fields},
};
use datafusion::{
    common::{Result as DataFusionResult, ScalarValue, exec_err},
    logical_expr::{
        Accumulator, AggregateUDF, AggregateUDFImpl, Signature, Volatility,
        function::{AccumulatorArgs, StateFieldsArgs},
    },
};
use std::{any::Any, cmp::Ordering, collections::BTreeMap, sync::Arc};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(super) struct OrderSpec {
    pub direction: SortDirection,
    pub nulls: NullOrdering,
}

impl From<&SortColumn> for OrderSpec {
    fn from(col: &SortColumn) -> Self {
        Self {
            direction: col.direction,
            nulls: col.nulls,
        }
    }
}

fn compare_with_spec(a: &ScalarValue, b: &ScalarValue, spec: OrderSpec) -> Ordering {
    match (a.is_null(), b.is_null()) {
        (true, true) => Ordering::Equal,
        (true, false) => match spec.nulls {
            NullOrdering::NullsFirst => Ordering::Less,
            NullOrdering::NullsLast => Ordering::Greater,
        },
        (false, true) => match spec.nulls {
            NullOrdering::NullsFirst => Ordering::Greater,
            NullOrdering::NullsLast => Ordering::Less,
        },
        (false, false) => {
            let ord = compare_non_null(a, b);
            match spec.direction {
                SortDirection::Asc => ord,
                SortDirection::Desc => ord.reverse(),
            }
        }
    }
}

fn compare_order_vectors(a: &[ScalarValue], b: &[ScalarValue], specs: &[OrderSpec]) -> Ordering {
    for ((va, vb), spec) in a.iter().zip(b.iter()).zip(specs.iter()) {
        let ord = compare_with_spec(va, vb, *spec);
        if !ord.is_eq() {
            return ord;
        }
    }
    Ordering::Equal
}

fn compare_nulls_last_asc(a: &ScalarValue, b: &ScalarValue) -> Ordering {
    match (a.is_null(), b.is_null()) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Greater,
        (false, true) => Ordering::Less,
        (false, false) => compare_non_null(a, b),
    }
}

/// Builds a `FirstValueBy` UDAF instance with the given SQL function name and sort specifications.
pub(super) fn make_first_value_by_udaf(name: String, order_specs: Vec<OrderSpec>) -> AggregateUDF {
    AggregateUDF::new_from_impl(FirstValueByUdaf {
        name,
        signature: Signature::any(1 + order_specs.len(), Volatility::Immutable),
        order_specs,
    })
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct FirstValueByUdaf {
    name: String,
    signature: Signature,
    order_specs: Vec<OrderSpec>,
}

impl AggregateUDFImpl for FirstValueByUdaf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> DataFusionResult<DataType> {
        if arg_types.is_empty() {
            return exec_err!("FirstValueBy requires at least one argument");
        }
        Ok(arg_types[0].clone())
    }

    fn state_fields(&self, args: StateFieldsArgs) -> DataFusionResult<Vec<FieldRef>> {
        let mut fields = Vec::with_capacity(args.input_fields.len() + 1);
        fields.push(Arc::new(
            Field::new("best_val", args.input_fields[0].data_type().clone(), true)
                .with_metadata(args.input_fields[0].metadata().clone()),
        ));
        for (idx, f) in args.input_fields.iter().skip(1).enumerate() {
            fields.push(Arc::new(Field::new(
                format!("best_ord_{idx}"),
                f.data_type().clone(),
                true,
            )));
        }
        fields.push(Arc::new(Field::new("has_value", DataType::Boolean, false)));
        Ok(fields)
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> DataFusionResult<Box<dyn Accumulator>> {
        let val_type = acc_args.expr_fields[0].data_type().clone();
        let ord_types = acc_args
            .expr_fields
            .iter()
            .skip(1)
            .map(|f| f.data_type().clone())
            .collect();
        Ok(Box::new(FirstValueByAccumulator {
            val_type,
            ord_types,
            order_specs: self.order_specs.clone(),
            best: None,
        }))
    }
}

#[derive(Debug)]
struct FirstValueByAccumulator {
    val_type: DataType,
    ord_types: Vec<DataType>,
    order_specs: Vec<OrderSpec>,
    best: Option<(ScalarValue, Vec<ScalarValue>)>,
}

impl FirstValueByAccumulator {
    fn consider(&mut self, cand_val: ScalarValue, cand_ords: Vec<ScalarValue>) {
        if cand_val.is_null() {
            return;
        }
        let replace = match &self.best {
            None => true,
            Some((best_val, best_ords)) => {
                let ord = compare_order_vectors(&cand_ords, best_ords, &self.order_specs)
                    .then_with(|| compare_non_null(&cand_val, best_val));
                ord.is_lt()
            }
        };
        if replace {
            self.best = Some((cand_val, cand_ords));
        }
    }
}

impl Accumulator for FirstValueByAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DataFusionResult<()> {
        if values.is_empty() {
            return Ok(());
        }
        let val_arr = &values[0];
        let ord_arrs = &values[1..];
        for row in 0..val_arr.len() {
            if val_arr.is_null(row) {
                continue;
            }
            let cand_val = ScalarValue::try_from_array(val_arr.as_ref(), row)?;
            let mut cand_ords = Vec::with_capacity(ord_arrs.len());
            for ord_arr in ord_arrs {
                cand_ords.push(ScalarValue::try_from_array(ord_arr.as_ref(), row)?);
            }
            self.consider(cand_val, cand_ords);
        }
        Ok(())
    }

    fn evaluate(&mut self) -> DataFusionResult<ScalarValue> {
        match &self.best {
            Some((val, _)) => Ok(val.clone()),
            None => ScalarValue::try_from(&self.val_type),
        }
    }

    fn size(&self) -> usize {
        let mut total = std::mem::size_of_val(self)
            + self.ord_types.capacity() * std::mem::size_of::<DataType>()
            + self.order_specs.capacity() * std::mem::size_of::<OrderSpec>();
        if let Some((val, ords)) = &self.best {
            total = total.saturating_add(val.size());
            for o in ords {
                total = total.saturating_add(o.size());
            }
        }
        total
    }

    fn state(&mut self) -> DataFusionResult<Vec<ScalarValue>> {
        let mut out = Vec::with_capacity(2 + self.ord_types.len());
        match &self.best {
            Some((val, ords)) => {
                out.push(val.clone());
                out.extend(ords.iter().cloned());
                out.push(ScalarValue::Boolean(Some(true)));
            }
            None => {
                out.push(ScalarValue::try_from(&self.val_type)?);
                for dt in &self.ord_types {
                    out.push(ScalarValue::try_from(dt)?);
                }
                out.push(ScalarValue::Boolean(Some(false)));
            }
        }
        Ok(out)
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DataFusionResult<()> {
        if states.len() < 2 {
            return Ok(());
        }
        let has_val_arr = states
            .last()
            .and_then(|a| a.as_any().downcast_ref::<BooleanArray>())
            .ok_or_else(|| {
                datafusion::common::DataFusionError::Internal(
                    "FirstValueBy state missing has_value boolean column".into(),
                )
            })?;
        let val_arr = &states[0];
        let ord_arrs = &states[1..states.len() - 1];
        for row in 0..val_arr.len() {
            if has_val_arr.is_null(row) || !has_val_arr.value(row) || val_arr.is_null(row) {
                continue;
            }
            let cand_val = ScalarValue::try_from_array(val_arr.as_ref(), row)?;
            let mut cand_ords = Vec::with_capacity(ord_arrs.len());
            for ord_arr in ord_arrs {
                cand_ords.push(ScalarValue::try_from_array(ord_arr.as_ref(), row)?);
            }
            self.consider(cand_val, cand_ords);
        }
        Ok(())
    }
}

/// Builds a `CollectDistinctWithProvenance` UDAF instance.
pub(super) fn make_collect_distinct_with_provenance_udaf(
    name: String,
    prov_names: Vec<String>,
    order_specs: Vec<OrderSpec>,
) -> AggregateUDF {
    let total_args = 1 + prov_names.len() + order_specs.len();
    AggregateUDF::new_from_impl(CollectDistinctWithProvenanceUdaf {
        name,
        signature: Signature::any(total_args, Volatility::Immutable),
        prov_names,
        order_specs,
    })
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct CollectDistinctWithProvenanceUdaf {
    name: String,
    signature: Signature,
    prov_names: Vec<String>,
    order_specs: Vec<OrderSpec>,
}

impl CollectDistinctWithProvenanceUdaf {
    fn output_item_field(&self, arg_types: &[DataType]) -> FieldRef {
        let mut struct_fields = Vec::with_capacity(1 + self.prov_names.len());
        struct_fields.push(Arc::new(Field::new("value", arg_types[0].clone(), false)));
        for (idx, p_name) in self.prov_names.iter().enumerate() {
            struct_fields.push(Arc::new(Field::new(
                p_name,
                arg_types[1 + idx].clone(),
                true,
            )));
        }
        Arc::new(Field::new(
            "item",
            DataType::Struct(Fields::from(struct_fields)),
            false,
        ))
    }

    fn state_item_field(&self, arg_types: &[DataType]) -> FieldRef {
        let num_prov = self.prov_names.len();
        let num_ord = self.order_specs.len();
        let mut struct_fields = Vec::with_capacity(1 + num_prov + num_ord);
        struct_fields.push(Arc::new(Field::new("value", arg_types[0].clone(), false)));
        for idx in 0..num_prov {
            struct_fields.push(Arc::new(Field::new(
                format!("prov_{idx}"),
                arg_types[1 + idx].clone(),
                true,
            )));
        }
        for idx in 0..num_ord {
            struct_fields.push(Arc::new(Field::new(
                format!("ord_{idx}"),
                arg_types[1 + num_prov + idx].clone(),
                true,
            )));
        }
        Arc::new(Field::new(
            "item",
            DataType::Struct(Fields::from(struct_fields)),
            false,
        ))
    }
}

impl AggregateUDFImpl for CollectDistinctWithProvenanceUdaf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Ok(DataType::List(self.output_item_field(arg_types)))
    }

    fn return_field(&self, arg_fields: &[FieldRef]) -> DataFusionResult<FieldRef> {
        let arg_types: Vec<DataType> = arg_fields.iter().map(|f| f.data_type().clone()).collect();
        Ok(Arc::new(Field::new(
            &self.name,
            DataType::List(self.output_item_field(&arg_types)),
            false,
        )))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> DataFusionResult<Vec<FieldRef>> {
        let arg_types: Vec<DataType> = args
            .input_fields
            .iter()
            .map(|f| f.data_type().clone())
            .collect();
        let state_item = self.state_item_field(&arg_types);
        Ok(vec![Arc::new(Field::new(
            "entries",
            DataType::List(state_item),
            false,
        ))])
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> DataFusionResult<Box<dyn Accumulator>> {
        let arg_types: Vec<DataType> = acc_args
            .expr_fields
            .iter()
            .map(|f| f.data_type().clone())
            .collect();
        Ok(Box::new(CollectDistinctWithProvenanceAccumulator {
            output_item_field: self.output_item_field(&arg_types),
            state_item_field: self.state_item_field(&arg_types),
            num_prov: self.prov_names.len(),
            order_specs: self.order_specs.clone(),
            entries: BTreeMap::new(),
            heap_bytes: 0,
        }))
    }
}

#[derive(Debug)]
struct CollectDistinctWithProvenanceAccumulator {
    output_item_field: FieldRef,
    state_item_field: FieldRef,
    num_prov: usize,
    order_specs: Vec<OrderSpec>,
    /// Keyed by distinct non-null `value` (sorted ascending by `OrdScalar`).
    /// Value is `(provenance_tuple, order_by_tuple)`.
    entries: BTreeMap<OrdScalar, (Vec<ScalarValue>, Vec<ScalarValue>)>,
    heap_bytes: usize,
}

impl CollectDistinctWithProvenanceAccumulator {
    fn entry_bytes(key: &OrdScalar, provs: &[ScalarValue], ords: &[ScalarValue]) -> usize {
        let mut bytes = 64 + key.0.size();
        for p in provs {
            bytes = bytes.saturating_add(p.size());
        }
        for o in ords {
            bytes = bytes.saturating_add(o.size());
        }
        bytes
    }

    fn consider(&mut self, val: ScalarValue, provs: Vec<ScalarValue>, ords: Vec<ScalarValue>) {
        if val.is_null() {
            return;
        }
        let key = OrdScalar(val);
        if let Some((best_provs, best_ords)) = self.entries.get_mut(&key) {
            let cmp = compare_order_vectors(&ords, best_ords, &self.order_specs).then_with(|| {
                for (pa, pb) in provs.iter().zip(best_provs.iter()) {
                    let ord = compare_nulls_last_asc(pa, pb);
                    if !ord.is_eq() {
                        return ord;
                    }
                }
                Ordering::Equal
            });
            if cmp.is_lt() {
                let old_bytes = Self::entry_bytes(&key, best_provs, best_ords);
                let new_bytes = Self::entry_bytes(&key, &provs, &ords);
                self.heap_bytes = self
                    .heap_bytes
                    .saturating_sub(old_bytes)
                    .saturating_add(new_bytes);
                *best_provs = provs;
                *best_ords = ords;
            }
        } else {
            let new_bytes = Self::entry_bytes(&key, &provs, &ords);
            self.heap_bytes = self.heap_bytes.saturating_add(new_bytes);
            self.entries.insert(key, (provs, ords));
        }
    }

    fn build_struct_list(&self, include_ords: bool) -> DataFusionResult<ScalarValue> {
        let item_field = if include_ords {
            &self.state_item_field
        } else {
            &self.output_item_field
        };
        let DataType::Struct(struct_fields) = item_field.data_type() else {
            return exec_err!("expected Struct item field in CollectDistinctWithProvenance");
        };
        if self.entries.is_empty() {
            let empty_struct = new_empty_array(item_field.data_type());
            let offsets = OffsetBuffer::from_lengths([0usize]);
            let list =
                GenericListArray::<i32>::try_new(item_field.clone(), offsets, empty_struct, None)?;
            return Ok(ScalarValue::List(Arc::new(list)));
        }

        let len = self.entries.len();
        let num_cols = struct_fields.len();
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(num_cols);

        // Column 0: distinct non-null values in ascending order.
        let val_scalars: Vec<ScalarValue> = self.entries.keys().map(|k| k.0.clone()).collect();
        let val_arr = scalars_to_array(val_scalars, &struct_fields[0])
            .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))?;
        columns.push(val_arr);

        // Provenance columns.
        for p_idx in 0..self.num_prov {
            let p_scalars: Vec<ScalarValue> = self
                .entries
                .values()
                .map(|(provs, _)| provs[p_idx].clone())
                .collect();
            let p_arr = scalars_to_array(p_scalars, &struct_fields[1 + p_idx])
                .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))?;
            columns.push(p_arr);
        }

        // Order-by columns (only for intermediate state).
        if include_ords {
            for o_idx in 0..self.order_specs.len() {
                let o_scalars: Vec<ScalarValue> = self
                    .entries
                    .values()
                    .map(|(_, ords)| ords[o_idx].clone())
                    .collect();
                let o_arr = scalars_to_array(o_scalars, &struct_fields[1 + self.num_prov + o_idx])
                    .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))?;
                columns.push(o_arr);
            }
        }

        let struct_arr: ArrayRef =
            Arc::new(StructArray::try_new(struct_fields.clone(), columns, None)?);
        let coerced = coerce_array_to_field(&struct_arr, item_field)
            .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))?;
        let offsets = OffsetBuffer::from_lengths([len]);
        let list = GenericListArray::<i32>::try_new(item_field.clone(), offsets, coerced, None)?;
        Ok(ScalarValue::List(Arc::new(list)))
    }
}

impl Accumulator for CollectDistinctWithProvenanceAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DataFusionResult<()> {
        if values.is_empty() {
            return Ok(());
        }
        let val_arr = &values[0];
        let prov_arrs = &values[1..=self.num_prov];
        let ord_arrs = &values[1 + self.num_prov..];
        for row in 0..val_arr.len() {
            if val_arr.is_null(row) {
                continue;
            }
            let val = ScalarValue::try_from_array(val_arr.as_ref(), row)?;
            let mut provs = Vec::with_capacity(self.num_prov);
            for pa in prov_arrs {
                provs.push(ScalarValue::try_from_array(pa.as_ref(), row)?);
            }
            let mut ords = Vec::with_capacity(ord_arrs.len());
            for oa in ord_arrs {
                ords.push(ScalarValue::try_from_array(oa.as_ref(), row)?);
            }
            self.consider(val, provs, ords);
        }
        Ok(())
    }

    fn evaluate(&mut self) -> DataFusionResult<ScalarValue> {
        self.build_struct_list(false)
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self).saturating_add(self.heap_bytes)
    }

    fn state(&mut self) -> DataFusionResult<Vec<ScalarValue>> {
        Ok(vec![self.build_struct_list(true)?])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DataFusionResult<()> {
        if states.is_empty() {
            return Ok(());
        }
        let list_arr = states[0]
            .as_any()
            .downcast_ref::<GenericListArray<i32>>()
            .ok_or_else(|| {
                datafusion::common::DataFusionError::Internal(
                    "CollectDistinctWithProvenance state is not a ListArray".into(),
                )
            })?;
        for list_row in 0..list_arr.len() {
            if list_arr.is_null(list_row) {
                continue;
            }
            let items = list_arr.value(list_row);
            let struct_arr = items
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| {
                    datafusion::common::DataFusionError::Internal(
                        "CollectDistinctWithProvenance state items are not a StructArray".into(),
                    )
                })?;
            let val_col = struct_arr.column(0);
            for item_row in 0..struct_arr.len() {
                if struct_arr.is_null(item_row) || val_col.is_null(item_row) {
                    continue;
                }
                let val = ScalarValue::try_from_array(val_col.as_ref(), item_row)?;
                let mut provs = Vec::with_capacity(self.num_prov);
                for p_idx in 0..self.num_prov {
                    provs.push(ScalarValue::try_from_array(
                        struct_arr.column(1 + p_idx).as_ref(),
                        item_row,
                    )?);
                }
                let mut ords = Vec::with_capacity(self.order_specs.len());
                for o_idx in 0..self.order_specs.len() {
                    ords.push(ScalarValue::try_from_array(
                        struct_arr.column(1 + self.num_prov + o_idx).as_ref(),
                        item_row,
                    )?);
                }
                self.consider(val, provs, ords);
            }
        }
        Ok(())
    }
}
