function changeFirm(firm) {

    $(function () {
		$('#container').highcharts({
			chart: {
				type: 'spline'
			},
			title: {
				text: 'Reputation de l\'entreprise '+firm+ " pendant une annee"
			},
			subtitle: {
				text: 'Source: WorldClimate.com'
			},
			xAxis: {
				categories: [
					'Jan',
					'Feb',
					'Mar',
					'Apr',
					'May'
				]
			},
			yAxis: {
				min: 0,
				title: {
					text: 'Reputation'
				}
			},
			tooltip: {
				headerFormat: '<span style="font-size:10px">{point.key}</span><table>',
				pointFormat: '<tr><td style="color:{series.color};padding:0">{series.name}: </td>' +
					'<td style="padding:0"><b>{point.y:.1f} mm</b></td></tr>',
				footerFormat: '</table>',
				shared: true,
				useHTML: true
			},
			plotOptions: {
				column: {
					pointPadding: 0.2,
					borderWidth: 0
				}
			},
			series: [{
				name: firm,
				data: [49.9, 71.5, 106.4, 129.2, 144.0]


			}]
		});
    });

    
$.ajax({
    type:"GET",
    url:"http://127.0.0.1:8081/mentions?" + $.param({"firm":"GOOGLE", "startDate":"2017-01-01", "endDate":"2017-01-30"}),
    contentType:"application/json; charset=utf-8",
    dataType:"json",
    origin:"telecomparis",
    success:function(data){
	var y = [];
	var dates = [];
	$.each(data,function(i,item){
	    dates.append(data[i].mentionDateTime);
	    y.append(data[i].mentionDocTone);
	});	
//	$('#country').append("<option value=''disabled selected>---Select country---</option>");
//	$.each(data,function(i,item){
//	    $('#country').append('<option value="' + data[i].country_id + '"name="' + data[i].country_code+ '">' + data[i].country_name + '</option>');
//	});
    

	    }
});

}
