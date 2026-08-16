use numpy::{PyArray2, PyReadonlyArray2, PyUntypedArrayMethods};
use pyo3::prelude::*;
use rayon::prelude::*;

fn median(mut values: Vec<f64>) -> f64 {
    values.sort_by(|left, right| left.total_cmp(right));
    let middle = values.len() / 2;
    if values.len().is_multiple_of(2) {
        (values[middle - 1] + values[middle]) / 2.0
    } else {
        values[middle]
    }
}

#[pyfunction]
fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

/// Cross-sectional rank with average ties, normalized to (1..N)/N.
/// NaN values receive NaN in the output. Operates row-wise with rayon parallelism.
#[pyfunction]
fn cross_section_rank<'py>(
    py: Python<'py>,
    values: PyReadonlyArray2<'py, f64>,
) -> Bound<'py, PyArray2<f64>> {
    let matrix = values.as_array();
    let (rows, cols) = (matrix.shape()[0], matrix.shape()[1]);
    let mut output = vec![f64::NAN; rows * cols];
    output
        .par_chunks_mut(cols)
        .enumerate()
        .for_each(|(row_idx, out_row)| {
            let input_row = matrix.row(row_idx);
            let mut indexed: Vec<(usize, f64)> = input_row
                .iter()
                .enumerate()
                .filter_map(|(i, &v)| if v.is_finite() { Some((i, v)) } else { None })
                .collect();
            if indexed.is_empty() {
                return;
            }
            indexed.sort_by(|a, b| a.1.total_cmp(&b.1));
            let count = indexed.len();
            let mut start = 0;
            while start < count {
                let mut stop = start + 1;
                while stop < count && indexed[stop].1 == indexed[start].1 {
                    stop += 1;
                }
                let rank = (((start + 1) + stop) as f64 / 2.0) / count as f64;
                for item in &indexed[start..stop] {
                    out_row[item.0] = rank;
                }
                start = stop;
            }
        });
    let result = ndarray::Array2::from_shape_vec((rows, cols), output).unwrap();
    PyArray2::from_owned_array(py, result)
}

/// Robust standardization using median/MAD clipping with ddof=1 std.
#[pyfunction]
fn robust_standardize<'py>(
    py: Python<'py>,
    values: PyReadonlyArray2<'py, f64>,
    k: f64,
) -> PyResult<Bound<'py, PyArray2<f64>>> {
    if !k.is_finite() || k <= 0.0 {
        return Err(pyo3::exceptions::PyValueError::new_err("k 必须是有限正数"));
    }
    let matrix = values.as_array();
    let (rows, cols) = (matrix.shape()[0], matrix.shape()[1]);
    let mut output = vec![f64::NAN; rows * cols];
    output
        .par_chunks_mut(cols)
        .enumerate()
        .for_each(|(row_idx, out_row)| {
            let input_row = matrix.row(row_idx);
            let clean: Vec<f64> = input_row
                .iter()
                .filter(|&&v| v.is_finite())
                .copied()
                .collect();
            if clean.is_empty() {
                return;
            }
            let center = median(clean.clone());
            let mad = median(clean.iter().map(|v| (v - center).abs()).collect()) * 1.4826;
            let clipped: Vec<f64> = clean
                .iter()
                .map(|v| {
                    if mad > 0.0 {
                        v.clamp(center - k * mad, center + k * mad)
                    } else {
                        *v
                    }
                })
                .collect();
            let mean = clipped.iter().sum::<f64>() / clipped.len() as f64;
            let variance = if clipped.len() > 1 {
                clipped.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / (clipped.len() - 1) as f64
            } else {
                0.0
            };
            let std = variance.sqrt();
            let mut ci = 0;
            for (col_idx, &v) in input_row.iter().enumerate() {
                if v.is_finite() {
                    out_row[col_idx] = if std > 0.0 {
                        (clipped[ci] - mean) / std
                    } else {
                        0.0
                    };
                    ci += 1;
                }
            }
        });
    let result = ndarray::Array2::from_shape_vec((rows, cols), output).unwrap();
    Ok(PyArray2::from_owned_array(py, result))
}

/// Weighted z-score with NaN handling. Weights and values must have matching shape.
#[pyfunction]
fn weighted_zscore<'py>(
    py: Python<'py>,
    values: PyReadonlyArray2<'py, f64>,
    weights: PyReadonlyArray2<'py, f64>,
) -> PyResult<Bound<'py, PyArray2<f64>>> {
    let v_arr = values.as_array();
    let w_arr = weights.as_array();
    if v_arr.shape() != w_arr.shape() {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "values 和 weights 形状必须一致",
        ));
    }
    let (rows, cols) = (v_arr.shape()[0], v_arr.shape()[1]);
    let mut output = vec![f64::NAN; rows * cols];
    output
        .par_chunks_mut(cols)
        .enumerate()
        .for_each(|(row_idx, out_row)| {
            let v_row = v_arr.row(row_idx);
            let w_row = w_arr.row(row_idx);
            let valid: Vec<(usize, f64, f64)> = v_row
                .iter()
                .zip(w_row.iter())
                .enumerate()
                .filter_map(|(i, (&v, &w))| {
                    if v.is_finite() && w.is_finite() && w > 0.0 {
                        Some((i, v, w))
                    } else {
                        None
                    }
                })
                .collect();
            let total_w: f64 = valid.iter().map(|t| t.2).sum();
            if total_w <= 0.0 {
                return;
            }
            let mean = valid.iter().map(|t| t.1 * t.2).sum::<f64>() / total_w;
            let variance = valid
                .iter()
                .map(|t| (t.1 - mean).powi(2) * t.2)
                .sum::<f64>()
                / total_w;
            for (i, v, _) in &valid {
                out_row[*i] = if variance > 0.0 {
                    (v - mean) / variance.sqrt()
                } else {
                    0.0
                };
            }
        });
    let result = ndarray::Array2::from_shape_vec((rows, cols), output).unwrap();
    Ok(PyArray2::from_owned_array(py, result))
}

fn rolling_impl(matrix: ndarray::ArrayView2<'_, f64>, window: usize, std: bool) -> Vec<f64> {
    let (rows, cols) = (matrix.shape()[0], matrix.shape()[1]);
    let minimum = std::cmp::max(2, window / 2);
    let mut output = vec![f64::NAN; rows * cols];
    output
        .par_chunks_mut(cols.max(1))
        .enumerate()
        .for_each(|(row_idx, out_row)| {
            for (col_idx, out) in out_row.iter_mut().enumerate() {
                let start = row_idx.saturating_sub(window - 1);
                let sample: Vec<f64> = (start..=row_idx)
                    .filter_map(|r| {
                        let v = matrix[(r, col_idx)];
                        if v.is_finite() {
                            Some(v)
                        } else {
                            None
                        }
                    })
                    .collect();
                if sample.len() < minimum {
                    continue;
                }
                let mean = sample.iter().sum::<f64>() / sample.len() as f64;
                if !std {
                    *out = mean;
                } else {
                    let variance = sample.iter().map(|v| (v - mean).powi(2)).sum::<f64>()
                        / (sample.len() - 1) as f64;
                    *out = variance.sqrt();
                }
            }
        });
    output
}

#[pyfunction]
fn rolling_mean<'py>(
    py: Python<'py>,
    values: PyReadonlyArray2<'py, f64>,
    window: usize,
) -> PyResult<Bound<'py, PyArray2<f64>>> {
    if window == 0 {
        return Err(pyo3::exceptions::PyValueError::new_err("window 必须大于 0"));
    }
    let matrix = values.as_array();
    let (rows, cols) = (matrix.shape()[0], matrix.shape()[1]);
    let flat = rolling_impl(matrix, window, false);
    let result = ndarray::Array2::from_shape_vec((rows, cols), flat).unwrap();
    Ok(PyArray2::from_owned_array(py, result))
}

#[pyfunction]
fn rolling_std<'py>(
    py: Python<'py>,
    values: PyReadonlyArray2<'py, f64>,
    window: usize,
) -> PyResult<Bound<'py, PyArray2<f64>>> {
    if window == 0 {
        return Err(pyo3::exceptions::PyValueError::new_err("window 必须大于 0"));
    }
    let matrix = values.as_array();
    let (rows, cols) = (matrix.shape()[0], matrix.shape()[1]);
    let flat = rolling_impl(matrix, window, true);
    let result = ndarray::Array2::from_shape_vec((rows, cols), flat).unwrap();
    Ok(PyArray2::from_owned_array(py, result))
}

#[pyfunction]
fn rolling_corr<'py>(
    py: Python<'py>,
    left: PyReadonlyArray2<'py, f64>,
    right: PyReadonlyArray2<'py, f64>,
    window: usize,
) -> PyResult<Bound<'py, PyArray2<f64>>> {
    if window == 0 || left.shape() != right.shape() {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "rolling_corr 参数非法",
        ));
    }
    let l_arr = left.as_array();
    let r_arr = right.as_array();
    let (rows, cols) = (l_arr.shape()[0], l_arr.shape()[1]);
    let minimum = std::cmp::max(3, window / 2);
    let mut output = vec![f64::NAN; rows * cols];
    output
        .par_chunks_mut(cols.max(1))
        .enumerate()
        .for_each(|(row_idx, out_row)| {
            for (col_idx, out) in out_row.iter_mut().enumerate() {
                let start = row_idx.saturating_sub(window - 1);
                let sample: Vec<(f64, f64)> = (start..=row_idx)
                    .filter_map(|r| {
                        let a = l_arr[(r, col_idx)];
                        let b = r_arr[(r, col_idx)];
                        if a.is_finite() && b.is_finite() {
                            Some((a, b))
                        } else {
                            None
                        }
                    })
                    .collect();
                if sample.len() < minimum {
                    continue;
                }
                let n = sample.len() as f64;
                let mean_a = sample.iter().map(|t| t.0).sum::<f64>() / n;
                let mean_b = sample.iter().map(|t| t.1).sum::<f64>() / n;
                let cov = sample
                    .iter()
                    .map(|t| (t.0 - mean_a) * (t.1 - mean_b))
                    .sum::<f64>();
                let var_a = sample.iter().map(|t| (t.0 - mean_a).powi(2)).sum::<f64>();
                let var_b = sample.iter().map(|t| (t.1 - mean_b).powi(2)).sum::<f64>();
                if var_a > 0.0 && var_b > 0.0 {
                    *out = cov / (var_a * var_b).sqrt();
                }
            }
        });
    let result = ndarray::Array2::from_shape_vec((rows, cols), output).unwrap();
    Ok(PyArray2::from_owned_array(py, result))
}

#[pymodule]
fn _quantmaster_kernel(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(version, module)?)?;
    module.add_function(wrap_pyfunction!(cross_section_rank, module)?)?;
    module.add_function(wrap_pyfunction!(robust_standardize, module)?)?;
    module.add_function(wrap_pyfunction!(weighted_zscore, module)?)?;
    module.add_function(wrap_pyfunction!(rolling_mean, module)?)?;
    module.add_function(wrap_pyfunction!(rolling_std, module)?)?;
    module.add_function(wrap_pyfunction!(rolling_corr, module)?)?;
    Ok(())
}
