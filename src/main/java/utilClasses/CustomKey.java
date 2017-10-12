package utilClasses;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CustomKey implements WritableComparable<CustomKey> {
    private Integer cityID;
    private Integer dataSetType;


    public CustomKey() {

    }

    public CustomKey(Integer cityID, Integer dataSetType) {
        super();
        this.cityID = cityID;
        this.dataSetType = dataSetType;
    }

    public Integer getCityId() {
        return cityID;
    }

    public Integer getDataSetType() {
        return dataSetType;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(cityID);
        out.writeInt(dataSetType);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        cityID = in.readInt();
        dataSetType = in.readInt();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((dataSetType == null) ? 0 : dataSetType.hashCode());
        result = prime * result + ((cityID == null) ? 0 : cityID.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        CustomKey other = (CustomKey) obj;
        if (dataSetType == null) {
            if (other.dataSetType != null)
                return false;
        } else if (!dataSetType.equals(other.dataSetType))
            return false;
        if (cityID == null) {
            if (other.cityID != null)
                return false;
        } else if (!cityID.equals(other.cityID))
            return false;
        return true;
    }

    @Override
    public int compareTo(CustomKey o) {
        int returnValue = compare(cityID, o.getCityId());
        if (returnValue != 0) {
            return returnValue;
        }
        return compare(dataSetType, o.getDataSetType());
    }

    public static int compare(int k1, int k2) {
        return (k1 < k2 ? -1 : (k1 == k2 ? 0 : 1));
    }

    @Override
    public String toString() {
        return "util.CustomKey [userId=" + cityID + ", dataSetType=" + dataSetType + "]";
    }
}

