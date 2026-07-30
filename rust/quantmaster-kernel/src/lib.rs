use pyo3::prelude::*;
use rayon::prelude::*;

type Matrix = Vec<Vec<Option<f64>>>;

fn finite_row(row: &[Option<f64>]) -> Vec<f64> {
    row.iter()
        .filter_map(|value| value.filter(|item| item.is_finite()))
        .collect()
}

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

#[pyfunction]
fn cross_section_rank(values: Matrix) -> Matrix {
    values
        .into_par_iter()
        .map(|row| {
            let mut indexed: Vec<(usize, f64)> = row
                .iter()
                .enumerate()
                .filter_map(|(index, value)| {
                    value
                        .filter(|item| item.is_finite())
                        .map(|item| (index, item))
                })
                .collect();
            indexed.sort_by(|left, right| left.1.total_cmp(&right.1));
            let mut output = vec![None; row.len()];
            let count = indexed.len();
            let mut start = 0;
            while start < count {
                let mut stop = start + 1;
                while stop < count && indexed[stop].1 == indexed[start].1 {
                    stop += 1;
                }
                let rank = (((start + 1) + stop) as f64 / 2.0) / count as f64;
                for item in &indexed[start..stop] {
                    output[item.0] = Some(rank);
                }
                start = stop;
            }
            output
        })
        .collect()
}

#[pyfunction]
fn robust_standardize(values: Matrix, k: f64) -> PyResult<Matrix> {
    if !k.is_finite() || k <= 0.0 {
        return Err(pyo3::exceptions::PyValueError::new_err("k 必须是有限正数"));
    }
    Ok(values
        .into_par_iter()
        .map(|row| {
            let clean = finite_row(&row);
            if clean.is_empty() {
                return vec![None; row.len()];
            }
            let center = median(clean.clone());
            let mad = median(clean.iter().map(|item| (item - center).abs()).collect()) * 1.4826;
            let clipped: Vec<f64> = clean
                .iter()
                .map(|item| {
                    if mad > 0.0 {
                        item.clamp(center - k * mad, center + k * mad)
                    } else {
                        *item
                    }
                })
                .collect();
            let mean = clipped.iter().sum::<f64>() / clipped.len() as f64;
            let variance = if clipped.len() > 1 {
                clipped
                    .iter()
                    .map(|item| (item - mean).powi(2))
                    .sum::<f64>()
                    / (clipped.len() - 1) as f64
            } else {
                0.0
            };
            let std = variance.sqrt();
            let mut clean_index = 0;
            row.iter()
                .map(|value| match value {
                    Some(item) if item.is_finite() => {
                        let clipped_value = clipped[clean_index];
                        clean_index += 1;
                        Some(if std > 0.0 {
                            (clipped_value - mean) / std
                        } else {
                            0.0
                        })
                    }
                    _ => None,
                })
                .collect()
        })
        .collect())
}

#[pyfunction]
fn weighted_zscore(values: Matrix, weights: Matrix) -> PyResult<Matrix> {
    if values.len() != weights.len()
        || values
            .iter()
            .zip(&weights)
            .any(|(left, right)| left.len() != right.len())
    {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "values 和 weights 形状必须一致",
        ));
    }
    Ok(values
        .into_par_iter()
        .zip(weights.into_par_iter())
        .map(|(row, weight)| {
            let valid: Vec<(usize, f64, f64)> = row
                .iter()
                .zip(&weight)
                .enumerate()
                .filter_map(|(index, (value, item_weight))| match (value, item_weight) {
                    (Some(value), Some(item_weight))
                        if value.is_finite() && item_weight.is_finite() && *item_weight > 0.0 =>
                    {
                        Some((index, *value, *item_weight))
                    }
                    _ => None,
                })
                .collect();
            let total_weight = valid.iter().map(|item| item.2).sum::<f64>();
            let mut output = vec![None; row.len()];
            if total_weight <= 0.0 {
                return output;
            }
            let mean = valid.iter().map(|item| item.1 * item.2).sum::<f64>() / total_weight;
            let variance = valid
                .iter()
                .map(|item| (item.1 - mean).powi(2) * item.2)
                .sum::<f64>()
                / total_weight;
            for (index, value, _) in valid {
                output[index] = Some(if variance > 0.0 {
                    (value - mean) / variance.sqrt()
                } else {
                    0.0
                });
            }
            output
        })
        .collect())
}

fn rolling(values: Matrix, window: usize, std: bool) -> PyResult<Matrix> {
    if window == 0 {
        return Err(pyo3::exceptions::PyValueError::new_err("window 必须大于 0"));
    }
    if values.is_empty() {
        return Ok(values);
    }
    let columns = values[0].len();
    if values.iter().any(|row| row.len() != columns) {
        return Err(pyo3::exceptions::PyValueError::new_err("矩阵行长度不一致"));
    }
    let rows = values.len();
    let minimum = std::cmp::max(2, window / 2);
    let columns_output: Vec<Vec<Option<f64>>> = (0..columns)
        .into_par_iter()
        .map(|column| {
            (0..rows)
                .map(|stop| {
                    let start = (stop + 1).saturating_sub(window);
                    let sample: Vec<f64> = (start..=stop)
                        .filter_map(|index| values[index][column].filter(|item| item.is_finite()))
                        .collect();
                    if sample.len() < minimum {
                        return None;
                    }
                    let mean = sample.iter().sum::<f64>() / sample.len() as f64;
                    if !std {
                        return Some(mean);
                    }
                    let variance = sample.iter().map(|item| (item - mean).powi(2)).sum::<f64>()
                        / (sample.len() - 1) as f64;
                    Some(variance.sqrt())
                })
                .collect()
        })
        .collect();
    Ok((0..rows)
        .map(|row| {
            (0..columns)
                .map(|column| columns_output[column][row])
                .collect()
        })
        .collect())
}

#[pyfunction]
fn rolling_mean(values: Matrix, window: usize) -> PyResult<Matrix> {
    rolling(values, window, false)
}

#[pyfunction]
fn rolling_std(values: Matrix, window: usize) -> PyResult<Matrix> {
    rolling(values, window, true)
}

#[pyfunction]
fn rolling_corr(left: Matrix, right: Matrix, window: usize) -> PyResult<Matrix> {
    if window == 0
        || left.len() != right.len()
        || left.iter().zip(&right).any(|(a, b)| a.len() != b.len())
    {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "rolling_corr 参数非法",
        ));
    }
    if left.is_empty() {
        return Ok(left);
    }
    let rows = left.len();
    let columns = left[0].len();
    if left.iter().any(|row| row.len() != columns) || right.iter().any(|row| row.len() != columns) {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "rolling_corr 只接受规则矩阵",
        ));
    }
    let minimum = std::cmp::max(3, window / 2);
    let by_column: Vec<Vec<Option<f64>>> = (0..columns)
        .into_par_iter()
        .map(|column| {
            (0..rows)
                .map(|stop| {
                    let start = (stop + 1).saturating_sub(window);
                    let sample: Vec<(f64, f64)> = (start..=stop)
                        .filter_map(|index| match (left[index][column], right[index][column]) {
                            (Some(a), Some(b)) if a.is_finite() && b.is_finite() => Some((a, b)),
                            _ => None,
                        })
                        .collect();
                    if sample.len() < minimum {
                        return None;
                    }
                    let mean_a =
                        sample.iter().map(|item| item.0).sum::<f64>() / sample.len() as f64;
                    let mean_b =
                        sample.iter().map(|item| item.1).sum::<f64>() / sample.len() as f64;
                    let covariance = sample
                        .iter()
                        .map(|item| (item.0 - mean_a) * (item.1 - mean_b))
                        .sum::<f64>();
                    let variance_a = sample
                        .iter()
                        .map(|item| (item.0 - mean_a).powi(2))
                        .sum::<f64>();
                    let variance_b = sample
                        .iter()
                        .map(|item| (item.1 - mean_b).powi(2))
                        .sum::<f64>();
                    if variance_a > 0.0 && variance_b > 0.0 {
                        Some(covariance / (variance_a * variance_b).sqrt())
                    } else {
                        None
                    }
                })
                .collect()
        })
        .collect();
    Ok((0..rows)
        .map(|row| (0..columns).map(|column| by_column[column][row]).collect())
        .collect())
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
