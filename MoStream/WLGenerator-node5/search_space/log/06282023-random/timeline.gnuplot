# User-defined variables
timestamp_offset = 1687982333
print "time offset: ", timestamp_offset

layout_size = 100

set terminal pdfcairo enhanced color dashed font "Helvetica, 20"
set terminal pdfcairo size 10,1.3*layout_size

load 'config.pal'

set datafile separator " "
set lmargin at screen 0.1

output_name ="timeline-".x1."-".x2.".pdf"
set output output_name

title_name = "timeline-".x1."-".x2

set xrange [x1:x2]

set multiplot layout layout_size,1 title title_name scale 1,1
set tmargin .5
set bmargin 1
set xtics font "Helvetica, 15"
set ytics font "Helvetica, 15"

set datafile separator ","

set xlabel "timeline [h]"
set ylabel "IP Simulate"

plot "compute_ip-output.log" using (($1-timestamp_offset)/3600):($2) with points ps 0.5 lc rgb "#3d77ab" notitle axis x1y1
