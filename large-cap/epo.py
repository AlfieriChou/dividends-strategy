# -*- coding: utf-8 -*-
import numpy as np
import pandas as pd
from scipy.linalg import solve

class Epo(object):
  '''
  计算epo
  '''
  @staticmethod
  def epo(x, signal, lambda_, method = "simple", w = None, anchor = None, normalize = True, endogenous = True):
    assert isinstance(method, str), "`method` must be a string."
    assert isinstance(lambda_, (int, float)), "`lambda` must be a number."
    assert isinstance(w, (int, float)), "`w` must be a number."
    assert isinstance(normalize, bool), "`normalize` must be a boolean."

    if method == "anchored" and anchor is None:
      raise ValueError("When the `anchored` method is chosen the `anchor` can't be `None`.")

    n = x.shape[1]
    vcov = x.cov()
    corr = x.corr()
    I = np.eye(n)
    V = np.zeros((n, n))
    np.fill_diagonal(V, vcov.values.diagonal())
    std = np.sqrt(V)
    s = signal
    a = anchor

    shrunk_cor = ((1 - w) * I @ corr.values) + (w * I)  # equation 7
    cov_tilde = std @ shrunk_cor @ std  # topic 2.II: page 11
    inv_shrunk_cov = solve(cov_tilde, np.eye(n))

    if method == "simple":
      epo = (1 / lambda_) * inv_shrunk_cov @ signal  # equation 16
    elif method == "anchored":
      if endogenous:
        gamma = np.sqrt(a.T @ cov_tilde @ a) / np.sqrt(s.T @ inv_shrunk_cov @ cov_tilde @ inv_shrunk_cov @ s)
        epo = inv_shrunk_cov @ (((1 - w) * gamma * s) + ((w * I @ V @ a)))
      else:
        epo = inv_shrunk_cov @ (((1 - w) * (1 / lambda_) * s) + ((w * I @ V @ a)))
    else:
      raise ValueError("`method` not accepted. Try `simple` or `anchored` instead.")

    if normalize:
      epo = [0 if a < 0 else a for a in epo]
      epo = epo / np.sum(epo)

    return epo

  # 根据EPO获取相对应的权重
  @staticmethod
  def get_epo_weights(prices, end_date):
    returns = prices.pct_change().dropna() # 计算收益率
    d = np.diag(returns.cov())
    a = (1/d) / (1/d).sum()
    # a= np.array([0.25,0.25,0.25,0.25])
    weights = epo(x = returns, signal = returns.mean(), lambda_ = 10, method = "anchored", w = 1, anchor=a)
    return weights
