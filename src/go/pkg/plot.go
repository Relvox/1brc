package pkg

import (
	"log"
	"strings"

	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/plotutil"
	"gonum.org/v1/plot/vg"
)

func Plot(events []TEvent) {
	zMap := map[string]int{}
	pointsMap := make(map[string]plotter.XYs)
	var maxID float64

	startTime := events[0].Time
	for _, event := range events {
		key := strings.Trim(event.Text, " 0123456789")
		id, ok := zMap[key]
		if !ok {
			id = len(zMap) + 1 // Start IDs at 1
			zMap[key] = id
		}
		if float64(id) > maxID {
			maxID = float64(id)
		}
		points := pointsMap[key]
		points = append(points, plotter.XY{X: float64(event.Time.Sub(startTime)), Y: float64(id)})
		pointsMap[key] = points
	}

	p := plot.New()
	p.Title.Text = "Events Over Time"
	p.X.Label.Text = "Timestamp"
	p.Y.Label.Text = "Event Type"
	p.Y.Tick.Marker = plot.ConstantTicks([]plot.Tick{}) // Prepare to set custom ticks

	// Set custom Y-axis ticks
	var ticks []plot.Tick
	for key, id := range zMap {
		ticks = append(ticks, plot.Tick{Value: float64(id), Label: key})
	}
	p.Y.Tick.Marker = plot.ConstantTicks(ticks)

	colors := plotutil.SoftColors
	i := 0
	for _, points := range pointsMap {
		if i >= len(colors) {
			i = 0
		}
		scatter, err := plotter.NewScatter(points)
		if err != nil {
			log.Panic(err)
		}
		scatter.GlyphStyle.Color = colors[i]
		p.Add(scatter)
		i++
	}

	// Adjust Y-axis and X-axis ranges to fit the data
	p.Y.Min = 0.5
	p.Y.Max = maxID + 0.5
	// p.X.Min = 0 // Adjust as needed
	// p.X.Max = 1 // Adjust as needed

	if err := p.Save(32*vg.Inch, 4*vg.Inch, "events.png"); err != nil {
		log.Panic(err)
	}
}
