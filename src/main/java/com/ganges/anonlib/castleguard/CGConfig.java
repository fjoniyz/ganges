package com.ganges.anonlib.castleguard;

public class CGConfig {
  /** Required number of tuples for a cluster to be complete */
  private int k;
  /** Maximum number of active tuples */
  private int delta;
  /** Maximum number of clusters that can be active */
  private int beta;
  /** The probability of ignoring a tuple */
  private double bigBeta;
  /** Number of values to use in the rolling average */
  private int mu;
  /** Required number of distinct sensitive attributes for a cluster to be complete */
  private int l;
  /** The 'scale' of tuple fudging */
  private double phi;
  /** Whether we want to enable differential privacy dp parameter in castle.py */
  private boolean useDiffPrivacy;

  public CGConfig(
      int k,
      int delta,
      int beta,
      double bigBeta,
      int mu,
      int l,
      double phi,
      boolean useDiffPrivacy) {
    this.k = k;
    this.delta = delta;
    this.beta = beta;
    this.bigBeta = bigBeta;
    this.mu = mu;
    this.l = l;
    this.phi = phi;
    this.useDiffPrivacy = useDiffPrivacy;
  }

  public int getK() {
    return k;
  }

  public void setK(int k) {
    this.k = k;
  }

  public int getDelta() {
    return delta;
  }

  public void setDelta(int delta) {
    this.delta = delta;
  }

  public int getBeta() {
    return beta;
  }

  public void setBeta(int beta) {
    this.beta = beta;
  }

  public double getBigBeta() {
    return bigBeta;
  }

  public void setBigBeta(double bigBeta) {
    this.bigBeta = bigBeta;
  }

  public int getMu() {
    return mu;
  }

  public void setMu(int mu) {
    this.mu = mu;
  }

  public int getL() {
    return l;
  }

  public void setL(int l) {
    this.l = l;
  }

  public double getPhi() {
    return phi;
  }

  public void setPhi(double phi) {
    this.phi = phi;
  }

  public boolean isUseDiffPrivacy() {
    return useDiffPrivacy;
  }

  public void setUseDiffPrivacy(boolean useDiffPrivacy) {
    this.useDiffPrivacy = useDiffPrivacy;
  }
}
