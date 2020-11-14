package de.tradingpulse.connector.iexcloud.service;

import java.util.List;

import retrofit2.Call;
import retrofit2.http.GET;
import retrofit2.http.Path;
import retrofit2.http.Query;

public interface IEXCloudService {

	@GET("account/metadata")
	Call<IEXCloudMetadata> fetchMetadata(@Query("token") String apiSecret);
	
	@GET("stock/{symbol}/previous")
	Call<IEXCloudOHLCVRecord> fetchOHLCVPrevious(@Path("symbol") String symbol, @Query("token") String apiToken);

	@GET("stock/{symbol}/chart/{range}")
	Call<List<IEXCloudOHLCVRecord>> fetchOHLCVRange(@Path("symbol") String symbol, @Path("range") String range, @Query("token") String apiToken);
}
