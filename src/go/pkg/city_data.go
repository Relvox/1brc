package pkg

type CityData struct {
	Min, Sum, Max int
	Count         int
	HK            HK
}

func (cd *CityData) Merge(other *CityData) {
	if other == nil {
		return
	}

	cd.Min = min(cd.Min, other.Min)
	cd.Max = max(cd.Max, other.Max)
	cd.Sum += other.Sum
	cd.Count += other.Count
}

func (cd *CityData) MergeValue(value int) {
	if value == 0 {
		return
	}

	cd.Min = min(cd.Min, value)
	cd.Max = max(cd.Max, value)
	cd.Sum += value
	cd.Count++
}

type HashKey = uint

type HK struct {
	Hash HashKey
	Key  []byte
}

type HKV struct {
	HK
	Value int
}
