/*
 * by LB
 * in Harbin Institute of Technology
 *
 * 2021 Spring Semester
 */

import java.math.BigDecimal;

/**
 * contain 2 field, BigDecimal avg and BigDecimal cou.
 */
public class Average {
    /*
    avg: average of records
    cou: number of records
     */
    private BigDecimal avg;
    private BigDecimal cou;

    public Average(BigDecimal avg, BigDecimal cou) {
        this.avg = avg;
        this.cou = cou;
    }

    public Average(String avg, String cou) {

        this.avg = new BigDecimal(avg);
        this.cou = new BigDecimal(cou);
    }

    /**
     * update this.avg by adding newValue, set scale to avgScale
     *
     * @param newValue new value to add
     * @param avgScale the scale of this.avg, between 0 to 2
     */
    public void calculateAverage(BigDecimal newValue, int avgScale)
    {
        BigDecimal total = avg.multiply(cou);
        total = total.add(newValue);
        cou = cou.add(new BigDecimal(1));
        avg = total.divide(cou, 2, BigDecimal.ROUND_HALF_UP).setScale(avgScale, BigDecimal.ROUND_HALF_UP);

    }

    public BigDecimal getAvg() {
        return avg;
    }

    public BigDecimal getCou() {
        return cou;
    }

    public void setAvg(BigDecimal avg) {
        this.avg = avg;
    }

    public void setCou(BigDecimal cou) {
        this.cou = cou;
    }
}
