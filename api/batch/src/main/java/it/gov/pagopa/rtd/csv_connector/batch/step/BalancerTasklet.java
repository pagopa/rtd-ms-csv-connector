package it.gov.pagopa.rtd.csv_connector.batch.step;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

@Slf4j
@Data
public class BalancerTasklet implements Tasklet, InitializingBean {

    private Boolean isEnabled;
    private String inputPath;
    private List<String> outputPaths;

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {

        if (isEnabled) {

            PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
            Resource[] inputResources = resolver.getResources(inputPath);

            Map<String, Resource> outputMap = outputPaths.stream().map(outputPath ->
                    Pair.of(outputPath, resolver.getResource(outputPath))).collect(
                    Collectors.toMap(Pair::getLeft, Pair::getRight));

            TreeMap<String, Long> dirsSizeMap = new TreeMap<>();

            outputMap.keySet().forEach(outputPath -> {
                try {
                    dirsSizeMap.put(outputPath, FileUtils.sizeOfDirectory(outputMap
                            .get(outputPath).getFile()));
                } catch (IOException e) {
                    log.error(e.getMessage(),e);
                }
            });

            for (Resource inputResource : inputResources) {

                Long inputSize = FileUtils.sizeOf(inputResource.getFile());

                String currentMax = dirsSizeMap.firstKey();
                Long currentMaxSize = dirsSizeMap.firstEntry().getValue();

                for (String key : dirsSizeMap.keySet()) {

                    if (currentMaxSize > dirsSizeMap.get(key)) {
                        currentMax = key;
                        currentMaxSize = dirsSizeMap.get(key);
                    }

                }


                String filename = inputResource.getFile().getAbsolutePath();
                filename = filename.replaceAll("\\\\", "/");
                String[] fileArr = filename.split("/");

                FileUtils.moveFile(inputResource.getFile(),
                        FileUtils.getFile(outputMap.get(currentMax)
                                .getFile().getAbsolutePath().concat("/")
                                .concat(fileArr[fileArr.length-1])));
                dirsSizeMap.put(currentMax, currentMaxSize+inputSize);

            }

        }

        return RepeatStatus.FINISHED;

    }

    @Override
    public void afterPropertiesSet() throws Exception {}
}
