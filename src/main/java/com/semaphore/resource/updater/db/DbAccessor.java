package com.semaphore.resource.updater.db;

import com.semaphore.resource.updater.core.ResourcePermit;
import com.semaphore.resource.updater.core.UpdateResourceParam;
import com.semaphore.resource.updater.cache.CacheAccessor;
import com.semaphore.resource.updater.exceptions.DataUnConsistentException;
import com.semaphore.resource.updater.exceptions.ResourceRunException;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallbackWithoutResult;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 *
 * @date 2021/10/12 3:28 下午
 */
@Slf4j
public class DbAccessor {
    private static final String COMMA = ",";

    private DataSource dataSource;
    private TransactionTemplate transactionTemplate;
    /**
     * 资源表名
     */
    private String dbResourceTableName;
    /**
     * 资源key的列名
     * 例如商品编号
     */
    private String dbResourceKeyColumnName;
    /**
     * 资源可用数量permit的列名
     */
    private String dbResourceAvailablePermitColumnName;
    /**
     * 资源预占数量permit的列表
     */
    private String dbResourcePreLockPermitColumnName;

    public DbAccessor(DataSource dataSource,
                      TransactionTemplate transactionTemplate,
                      String dbResourceTableName,
                      String dbResourceKeyColumnName,
                      String dbResourceAvailablePermitColumnName,
                      String dbResourcePreLockPermitColumnName){
        this.dataSource = dataSource;
        this.transactionTemplate = transactionTemplate;
        this.dbResourceTableName = dbResourceTableName;
        this.dbResourceKeyColumnName = dbResourceKeyColumnName;
        this.dbResourceAvailablePermitColumnName = dbResourceAvailablePermitColumnName;
        this.dbResourcePreLockPermitColumnName = dbResourcePreLockPermitColumnName;
    }

    /**
     * QUERY ONE
     */
    private static final String QUERY_ONE_RESOURCE = "SELECT "
            + "%s,%s,%s"
            + " FROM "
            + "%s"
            + " WHERE %s = ?";

    /**
     * QUERY BATCH
     */
    private static final String QUERY_BATCH_RESOURCE = "SELECT "
            + "%s,%s,%s"
            + " FROM "
            + "%s"
            + " WHERE %s IN (?)";

    /**
     * UPDATE : SUBTRACT AVAILABLE
     */
    private static final String SUBTRACT_ONE_AVAILABLE_RESOURCE = "UPDATE "
            + "%s"
            + " SET "
            + "%s = (%s - ?)"
            +  " WHERE "
            + "%s = ? AND (%s - ?) >= 0";

    /**
     * UPDATE : SUBTRACT PRE LOCK
     */
    private static final String SUBTRACT_ONE_PRE_LOCK_RESOURCE = "UPDATE "
            + "%s"
            + " SET "
            + "%s = (%s - ?)"
            +  " WHERE "
            + "%s = ? AND (%s - ?) >= 0";

    /**
     * 增加可用资源数量
     */
    private static final String ADD_ONE_AVAILABLE_RESOURCE = "UPDATE "
            + "%s"
            + " SET "
            + "%s = (%s + ?)"
            +  " WHERE "
            + "%s = ?";

    /**
     * 增加预占资源数量
     */
    private static final String ADD_ONE_PRE_LOCK_RESOURCE = "UPDATE "
            + "%s"
            + " SET "
            + "%s = (%s + ?)"
            +  " WHERE "
            + "%s = ?";

    /**
     * 扣减可用资源 增加预占资源
     */
    private static final String SUBTRACT_AVAILABLE_ADD_PRE_LOCK = "UPDATE "
            + "%s"
            + " SET "
            + "%s = (%s - ?), %s = (%s + ?)"
            + " WHERE " + " %s = ? AND (%s - ?) >= 0";

    /**
     * 扣减预占资源 增加可用资源
     */
    private static final String SUBTRACT_PRE_LOCK_ADD_AVAILABLE = "UPDATE "
            + "%s"
            + " SET "
            + "%s = (%s - ?), %s = (%s + ?)"
            + " WHERE " + " %s = ? AND (%s - ?) >= 0";

    public ResourcePermit queryOneResource(String resourceId){
        log.info("db查询资源:" + resourceId);
        String sqlStr = String.format(QUERY_ONE_RESOURCE, dbResourceKeyColumnName, dbResourceAvailablePermitColumnName, dbResourcePreLockPermitColumnName, dbResourceTableName, dbResourceKeyColumnName);
        ResourcePermit permit = null;
        try(Connection connection = dataSource.getConnection()) {
            try(PreparedStatement ps = connection.prepareStatement(sqlStr)) {
                ps.setString(1,resourceId);
                ResultSet resultSet = ps.executeQuery();
                if(resultSet.next()){
                    permit = extractResourcePermit(resultSet);
                    return permit;
                }else {
                    return null;
                }
            }
        }catch (Exception e){
            throw new ResourceRunException("查询资源:" + resourceId + "失败，原因:" + e.getMessage());
        }
    }

    public List<ResourcePermit> queryResource(List<String> resourceIdList){
        if(Objects.isNull(resourceIdList)){
            return null;
        }
        log.info("db查询资源:" + String.join(",",resourceIdList));
        String sqlStr = String.format(QUERY_BATCH_RESOURCE, dbResourceKeyColumnName, dbResourceAvailablePermitColumnName, dbResourcePreLockPermitColumnName, dbResourceTableName, dbResourceKeyColumnName);
        final StringBuilder sb = new StringBuilder(
                String.join(", ", Collections.nCopies(resourceIdList.size(), "?")));
        sqlStr = sqlStr.replace("?",sb);

        List<ResourcePermit> resourcePermitList = new ArrayList<>();
        try(Connection connection = dataSource.getConnection()) {
            try(PreparedStatement ps = connection.prepareStatement(sqlStr)) {
                for(int i=0 ; i<resourceIdList.size() ; i++){
                    ps.setString(i+1,resourceIdList.get(i));
                }
                ResultSet resultSet = ps.executeQuery();
                while (resultSet.next()){
                    ResourcePermit permit = extractResourcePermit(resultSet);
                    resourcePermitList.add(permit);
                }
            }
        }catch (Exception e){
            throw new ResourceRunException("批量查询资源失败，原因:" + e.getMessage());
        }
        return resourcePermitList;
    }

    /**
     * 单个扣减可用资源
     * @param updateResourceParam
     */
    public void subtractOneAvailableResource(UpdateResourceParam updateResourceParam){
        transactionTemplate.execute(new TransactionCallbackWithoutResult() {
            @SneakyThrows
            @Override
            protected void doInTransactionWithoutResult(TransactionStatus status) {
                doSubtractOneAvailableResource(updateResourceParam);
            }
        });
    }

    /**
     * 单个扣减可用资源
     * @param updateResourceParam
     * @throws DataUnConsistentException
     */
    private void doSubtractOneAvailableResource(UpdateResourceParam updateResourceParam)
            throws DataUnConsistentException {
        String resourceId = updateResourceParam.getResourceId();
        Connection connection = DataSourceUtils.getConnection(dataSource);
        String sqlStr = String.format(SUBTRACT_ONE_AVAILABLE_RESOURCE, dbResourceTableName, dbResourceAvailablePermitColumnName, dbResourceAvailablePermitColumnName, dbResourceKeyColumnName, dbResourceAvailablePermitColumnName);
        int updateNum = 0;
        try(PreparedStatement ps = connection.prepareStatement(sqlStr)) {
            ps.setInt(1,updateResourceParam.getNum());
            ps.setString(2,resourceId);
            ps.setInt(3,updateResourceParam.getNum());
            updateNum = ps.executeUpdate();
        }catch (Exception e){
            throw new ResourceRunException("db扣减可用资源失败，原因:" + e.getMessage());
        }
        if(updateNum <= 0){
            //db和cache不一致了
            CacheAccessor.setAvailableResourceNotConsistence(resourceId);
            log.error("可用资源:{},cache和db出现不一致，将尝试限制业务请求并进行校准",resourceId);
            throw new DataUnConsistentException("db扣减可用资源:" + resourceId + "失败，cache和db出现不一致，将尝试限制业务请求并进行校准");
        }
    }

    /**
     * 批量扣减可用资源
     * @param updateResourceParamList
     */
    public void subtractAvailableResource(List<UpdateResourceParam> updateResourceParamList){
        transactionTemplate.execute(new TransactionCallbackWithoutResult() {
            @SneakyThrows
            @Override
            protected void doInTransactionWithoutResult(TransactionStatus status) {
                for(UpdateResourceParam updateResourceParam : updateResourceParamList){
                    doSubtractOneAvailableResource(updateResourceParam);
                }
            }
        });
    }

    private ResourcePermit extractResourcePermit(ResultSet resultSet) throws SQLException {
        ResourcePermit resourcePermit = new ResourcePermit();
        Integer availablePermit = resultSet.getInt(dbResourceAvailablePermitColumnName);
        Integer preLockPermit = resultSet.getInt(dbResourcePreLockPermitColumnName);
        String resourceId = resultSet.getString(dbResourceKeyColumnName);

        resourcePermit.setAvailableCount(availablePermit);
        resourcePermit.setPreLockCount(preLockPermit);
        resourcePermit.setResourceId(resourceId);
        return resourcePermit;
    }

    /**
     * 批量扣减可用资源 并 添加预占资源
     * @param updateResourceParamList
     */
    public void subtractAvailableAndAddPreLockResource(List<UpdateResourceParam> updateResourceParamList) {
        transactionTemplate.execute(new TransactionCallbackWithoutResult() {
            @SneakyThrows
            @Override
            protected void doInTransactionWithoutResult(TransactionStatus status) {
                for(UpdateResourceParam updateResourceParam : updateResourceParamList){
                    doSubtractAvailableAndAddOnePreLockResource(updateResourceParam);
                }
            }
        });
    }

    /**
     * 单个扣减可用资源 并 增加预占资源
     * @param updateResourceParam
     * @throws DataUnConsistentException
     */
    private void doSubtractAvailableAndAddOnePreLockResource(UpdateResourceParam updateResourceParam) throws DataUnConsistentException {
        String resourceId = updateResourceParam.getResourceId();
        Connection connection = DataSourceUtils.getConnection(dataSource);
        String sqlStr = String.format(SUBTRACT_AVAILABLE_ADD_PRE_LOCK, dbResourceTableName, dbResourceAvailablePermitColumnName, dbResourceAvailablePermitColumnName,
                dbResourcePreLockPermitColumnName, dbResourcePreLockPermitColumnName,
                dbResourceKeyColumnName, dbResourceAvailablePermitColumnName);
        int updateNum = 0;
        try(PreparedStatement ps = connection.prepareStatement(sqlStr)) {
            ps.setInt(1,updateResourceParam.getNum());
            ps.setInt(2,updateResourceParam.getNum());
            ps.setString(3,resourceId);
            ps.setInt(4,updateResourceParam.getNum());
            updateNum = ps.executeUpdate();
        }catch (Exception e){
            throw new ResourceRunException("db扣减可用资源失败，原因:" + e.getMessage());
        }
        if(updateNum <= 0){
            //db和cache不一致了
            CacheAccessor.setAvailableResourceNotConsistence(resourceId);
            log.error("可用资源:{},cache和db出现不一致，将尝试限制业务请求并进行校准",resourceId);
            throw new DataUnConsistentException("db扣减可用资源:" + resourceId + "失败，cache和db出现不一致，将尝试限制业务请求并进行校准");
        }
    }

    /**
     * 批量扣减预占资源 并 添加可用资源
     * @see DbAccessor#subtractAvailableAndAddPreLockResource 的逆向
     * @param updateResourceParamList
     */
    public void subtractPreLockedAndAddAvailableResource(List<UpdateResourceParam> updateResourceParamList) {
        transactionTemplate.execute(new TransactionCallbackWithoutResult() {
            @SneakyThrows
            @Override
            protected void doInTransactionWithoutResult(TransactionStatus status) {
                for(UpdateResourceParam updateResourceParam : updateResourceParamList){
                    doSubtractPreLockedAndAddAvailableResource(updateResourceParam);
                }
            }
        });
    }

    /**
     * 单个扣减预占资源 并 添加可用资源
     * @param updateResourceParam
     * @throws DataUnConsistentException
     */
    private void doSubtractPreLockedAndAddAvailableResource(UpdateResourceParam updateResourceParam) throws DataUnConsistentException {
        String resourceId = updateResourceParam.getResourceId();
        Connection connection = DataSourceUtils.getConnection(dataSource);
        String sqlStr = String.format(SUBTRACT_PRE_LOCK_ADD_AVAILABLE,dbResourceTableName, dbResourcePreLockPermitColumnName, dbResourcePreLockPermitColumnName,
                dbResourceAvailablePermitColumnName, dbResourceAvailablePermitColumnName,
                dbResourceKeyColumnName, dbResourcePreLockPermitColumnName);
        int updateNum = 0;
        try(PreparedStatement ps = connection.prepareStatement(sqlStr)) {
            ps.setInt(1,updateResourceParam.getNum());
            ps.setInt(2,updateResourceParam.getNum());
            ps.setString(3,resourceId);
            ps.setInt(4,updateResourceParam.getNum());
            updateNum = ps.executeUpdate();
        }catch (Exception e){
            throw new ResourceRunException("db扣减预占资源失败，原因:" + e.getMessage());
        }
        if(updateNum <= 0){
            //db和cache不一致了
            CacheAccessor.setPreLockedResourceNotConsistence(resourceId);
            log.error("预占资源:{},cache和db出现不一致，将尝试限制业务请求并进行校准",resourceId);
            throw new DataUnConsistentException("db扣减预占资源:" + resourceId + "失败，cache和db出现不一致，将尝试限制业务请求并进行校准");
        }
    }

    /**
     * 批量增加可用资源数量
     * @param updateResourceParamList
     */
    public void addAvailableResource(List<UpdateResourceParam> updateResourceParamList) {
        transactionTemplate.execute(new TransactionCallbackWithoutResult() {
            @SneakyThrows
            @Override
            protected void doInTransactionWithoutResult(TransactionStatus status) {
                for(UpdateResourceParam updateResourceParam : updateResourceParamList){
                    doAddAvailableResource(updateResourceParam);
                }
            }
        });
    }

    /**
     * 单个增加可用资源数量
     * @param updateResourceParam
     */
    private void doAddAvailableResource(UpdateResourceParam updateResourceParam) {
        String resourceId = updateResourceParam.getResourceId();
        Connection connection = DataSourceUtils.getConnection(dataSource);
        String sqlStr = String.format(ADD_ONE_AVAILABLE_RESOURCE, dbResourceTableName, dbResourceAvailablePermitColumnName, dbResourceAvailablePermitColumnName, dbResourceKeyColumnName);
        try(PreparedStatement ps = connection.prepareStatement(sqlStr)) {
            ps.setInt(1,updateResourceParam.getNum());
            ps.setString(2,resourceId);
            ps.executeUpdate();
        }catch (Exception e){
            throw new ResourceRunException("db添加可用资源失败，原因:" + e.getMessage());
        }
    }

    /**
     * 扣减预占资源
     * @param updateResourceParamList
     */
    public void subtractPreLockedResource(List<UpdateResourceParam> updateResourceParamList) {
        transactionTemplate.execute(new TransactionCallbackWithoutResult() {
            @SneakyThrows
            @Override
            protected void doInTransactionWithoutResult(TransactionStatus status) {
                for(UpdateResourceParam updateResourceParam : updateResourceParamList){
                    doSubtractOnePreLockResource(updateResourceParam);
                }
            }
        });
    }

    /**
     * 单个扣减预占资源
     * @param updateResourceParam
     * @throws DataUnConsistentException
     */
    private void doSubtractOnePreLockResource(UpdateResourceParam updateResourceParam)
            throws DataUnConsistentException {
        String resourceId = updateResourceParam.getResourceId();
        Connection connection = DataSourceUtils.getConnection(dataSource);
        String sqlStr = String.format(SUBTRACT_ONE_PRE_LOCK_RESOURCE, dbResourceTableName, dbResourcePreLockPermitColumnName, dbResourcePreLockPermitColumnName, dbResourceKeyColumnName, dbResourcePreLockPermitColumnName);
        int updateNum = 0;
        try(PreparedStatement ps = connection.prepareStatement(sqlStr)) {
            ps.setInt(1,updateResourceParam.getNum());
            ps.setString(2,resourceId);
            ps.setInt(3,updateResourceParam.getNum());
            updateNum = ps.executeUpdate();
        }catch (Exception e){
            throw new ResourceRunException("db扣减预占资源失败，原因:" + e.getMessage());
        }
        if(updateNum <= 0){
            //db和cache不一致了
            CacheAccessor.setPreLockedResourceNotConsistence(resourceId);
            log.error("预占资源:{},cache和db出现不一致，将尝试限制业务请求并进行校准",resourceId);
            throw new DataUnConsistentException("db扣减预占资源:" + resourceId + "失败，cache和db出现不一致，将尝试限制业务请求并进行校准");
        }
    }

    /**
     * 增加预占资源数量
     * @param updateResourceParamList
     */
    public void addPreLockResource(List<UpdateResourceParam> updateResourceParamList) {
        transactionTemplate.execute(new TransactionCallbackWithoutResult() {
            @SneakyThrows
            @Override
            protected void doInTransactionWithoutResult(TransactionStatus status) {
                for(UpdateResourceParam updateResourceParam : updateResourceParamList){
                    doAddPreLockResource(updateResourceParam);
                }
            }
        });
    }

    /**
     * 单个增加预占资源数量
     * @param updateResourceParam
     */
    private void doAddPreLockResource(UpdateResourceParam updateResourceParam) {
        String resourceId = updateResourceParam.getResourceId();
        Connection connection = DataSourceUtils.getConnection(dataSource);
        String sqlStr = String.format(ADD_ONE_PRE_LOCK_RESOURCE, dbResourceTableName, dbResourcePreLockPermitColumnName, dbResourcePreLockPermitColumnName, dbResourceKeyColumnName);
        try(PreparedStatement ps = connection.prepareStatement(sqlStr)) {
            ps.setInt(1,updateResourceParam.getNum());
            ps.setString(2,resourceId);
            ps.executeUpdate();
        }catch (Exception e){
            throw new ResourceRunException("db添加预占资源失败，原因:" + e.getMessage());
        }
    }
}
