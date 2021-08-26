package cn.orvillex.observerdemo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.shaded.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.util.Bytes;

import cn.orvillex.observerdemo.Sum.SumRequest;
import cn.orvillex.observerdemo.Sum.SumResponse;

public class SumEndPoint extends Sum.SumService implements Coprocessor, CoprocessorService {
    private RegionCoprocessorEnvironment env;
    
    @Override
    public Service getService() {
        return this;
    }

    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
        if (env instanceof RegionCoprocessorEnvironment) {
            this.env = (RegionCoprocessorEnvironment)env;
        } else {
            throw new CoprocessorException("Must be loaded on a table region!");
        }
    }

    @Override
    public void stop(CoprocessorEnvironment env) throws IOException {

    }

    @Override
    public void getSum(RpcController controller, SumRequest request, RpcCallback<SumResponse> done) {
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(request.getFamily()));
        scan.addColumn(Bytes.toBytes(request.getFamily()), Bytes.toBytes(request.getColumn()));

        Sum.SumResponse response = null;
        InternalScanner scanner = null;

        try {
            scanner = env.getRegion().getScanner(scan);
            List<Cell> results = new ArrayList<>();
            boolean hasMore = false;
            long sum = 0L;

            do {
                hasMore = scanner.next(results);
                for (Cell cell : results) {
                    sum = sum + Bytes.toLong(cell.getValueArray());
                }
                results.clear();
            } while (hasMore);

            response = Sum.SumResponse.newBuilder().setSum(sum).build();
        } catch (IOException ioe) {
            ResponseConverter.setControllerException(controller, ioe);
        } finally {
            if (scanner != null) {
                try {
                    scanner.close();
                } catch (IOException ignored) {}
            }
        }

        done.run(response);
    }
    
}
