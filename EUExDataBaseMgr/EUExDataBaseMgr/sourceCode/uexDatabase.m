/**
 *
 *	@file   	: uexDatabase.m  in EUExDataBaseMgr
 *
 *	@author 	: CeriNo
 * 
 *	@date   	: 16/8/1
 *
 *	@copyright 	: 2016 The AppCan Open Source Project.
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Lesser General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Lesser General Public License for more details.
 *  You should have received a copy of the GNU Lesser General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */


#import "uexDatabase.h"
#import <sqlite3.h>
#import <AppCanKit/ACEXTScope.h>
@interface uexDatabase()
@property (nonatomic,assign)sqlite3 *dbHandle;

@property (nonatomic,strong)NSString *dbName;
@property (nonatomic,strong)dispatch_queue_t queue;
@property (nonatomic,strong)dispatch_semaphore_t transactionLock;
@property (nonatomic,assign)BOOL shouldRollback;
@end

@implementation uexDatabase


#define UEX_DO_IN_SERIAL_QUEUE_BEGIN    dispatch_async(self.queue, ^{
#define UEX_DO_IN_SERIAL_QUEUE_END      });

static NSString *kDatabaseFolderPath = nil;




+ (void)initialize{
    static dispatch_once_t onceToken;
    dispatch_once(&onceToken, ^{
        NSString * documentPath = NSSearchPathForDirectoriesInDomains(NSDocumentDirectory, NSUserDomainMask, YES).firstObject ;
        NSString *dbFolderPath = [documentPath stringByAppendingPathComponent:@"database"];
        BOOL isFolder = NO;
        BOOL isExist = [[NSFileManager defaultManager] fileExistsAtPath:dbFolderPath isDirectory:&isFolder];
        if (!isExist || !isFolder) {
            [[NSFileManager defaultManager] createDirectoryAtPath:dbFolderPath withIntermediateDirectories:YES attributes:nil error:nil];
        }
        kDatabaseFolderPath = dbFolderPath;
    });
}


+ (NSString *)dbFolderPath{
    return kDatabaseFolderPath;
}

- (BOOL)open:(NSString *)dbName{
    
    NSString *dbPath = nil;
    NSFileManager *fmanager = [NSFileManager defaultManager];
    if ([fmanager fileExistsAtPath:dbName]) {
        dbPath = dbName;
    } else {
        dbPath = [kDatabaseFolderPath stringByAppendingPathComponent:dbName];
    }
    
    if (sqlite3_open([dbPath UTF8String], &_dbHandle) == SQLITE_OK) {
        self.dbName = dbName;
        NSString *label = [@"com.appcan.uexDataBaseMgr.dbQueue." stringByAppendingString:dbName];
        self.queue = dispatch_queue_create([label UTF8String], DISPATCH_QUEUE_SERIAL);
        self.transactionLock = dispatch_semaphore_create(1);
        return YES;
    }else {
        sqlite3_close(_dbHandle);
    }
    return NO;
}
- (BOOL)close{
    if (sqlite3_close(_dbHandle) == SQLITE_OK) {
        _dbHandle = NULL;
        return YES;
    }
    return NO;
}

- (void)execSQL:(NSString *)SQL completion:(void (^)(BOOL))completion{
    if(!_dbHandle){
        if (completion) {
            completion(NO);
        }
        return;
    }
    
    
    UEX_DO_IN_SERIAL_QUEUE_BEGIN;
    char *errMsg = nil;
    int status = sqlite3_exec(_dbHandle, SQL.UTF8String, NULL, NULL, &errMsg);
    if (status == SQLITE_OK) {
        ACLogDebug(@"DB<%@> -> exec SQL '%@' SUCCESS!",self.dbName,SQL);
        completion(YES);
    }else{
        ACLogDebug(@"DB<%@> -> exec SQL '%@' FAILED! errStatus: %d errMsg: %s",self.dbName,SQL,status,errMsg);
        self.shouldRollback = YES;
        completion(NO);
    }
    UEX_DO_IN_SERIAL_QUEUE_END;
    
}

- (void)selectSQL:(NSString *)SQL completion:(void (^)(NSArray<NSDictionary *> *))completion{
    UEX_DO_IN_SERIAL_QUEUE_BEGIN;
    __block NSMutableArray<NSDictionary *> *result = nil;
    @onExit{
        completion(result);
    };
    
    sqlite3_stmt *stmt;
    if (sqlite3_prepare_v2(_dbHandle, SQL.UTF8String, -1, &stmt, NULL) != SQLITE_OK) {
        ACLogDebug(@"DB<%@> -> exec SQL '%@' FAILED! errMsg: %s",self.dbName,SQL,sqlite3_errmsg (_dbHandle));
        return;
    }
    result = [NSMutableArray array];
    BOOL keepGoing = YES,isSuccess = YES;
    while (keepGoing) {
        int stepStatus = sqlite3_step(stmt);
        switch (stepStatus) {
            case SQLITE_ROW:{
                NSMutableDictionary *entry = [NSMutableDictionary dictionary];
                int count = sqlite3_column_count(stmt);
                for(int i = 0;i<count;i++){
                    int column_type = sqlite3_column_type(stmt, i);
                    NSString *columnName = [NSString stringWithUTF8String:sqlite3_column_name(stmt, i)];
                    id obj = nil;
                    switch (column_type) {
                        case SQLITE_INTEGER:
                            obj = @(sqlite3_column_int(stmt, i));
                            break;
                        case SQLITE_TEXT:
                            obj = [NSString stringWithUTF8String:(const char*)sqlite3_column_text(stmt, i)];
                            break;
                        case SQLITE_FLOAT:
                            obj = @(sqlite3_column_double(stmt, i));
                            break;
                        default:{
                            break;
                        }
                    }
                    [entry setValue:obj forKey:columnName];
                }
                [result addObject:entry];
                break;
            }
            case SQLITE_DONE:{
                keepGoing = NO;
                break;
            }
            default:{
                isSuccess = NO;
                ACLogDebug(@"DB<%@> -> exec SQL '%@' FAILED! errMsg: stmt error",self.dbName,SQL);
                break;
            }
        }
    }
    sqlite3_finalize(stmt);
    if (!isSuccess) {
        result = nil;
    }
    UEX_DO_IN_SERIAL_QUEUE_END;
}

- (void)doTransactionWithSQLs:(NSArray<NSString *> *)SQLs completion:(void (^)(uexDatabaseTransactionResult))completion{
    UEX_DO_IN_SERIAL_QUEUE_BEGIN;
    __block uexDatabaseTransactionResult result = uexDatabaseTransactionError;
    @onExit{
        completion(result);
    };
    if (sqlite3_exec(_dbHandle, "BEGIN TRANSACTION", NULL, NULL, NULL) != SQLITE_OK) {
        return;
    }
    for (NSString *sql in SQLs) {
        char *errMsg = nil;
        int ret = sqlite3_exec(_dbHandle, sql.UTF8String, NULL, NULL, &errMsg);
        if (ret != SQLITE_OK) {
            result = uexDatabaseTransactionRollback;
            sqlite3_exec(_dbHandle, "ROLLBACK TRANSACTION", NULL, NULL, NULL);
            ACLogDebug(@"transaction exec '%@' error: %s",sql,errMsg);
            return;
        }
    }
    if (sqlite3_exec(_dbHandle, "COMMIT TRANSACTION", NULL, NULL, NULL) != SQLITE_OK) {
        return;
    }
    result = uexDatabaseTransactionSuccess;
    UEX_DO_IN_SERIAL_QUEUE_END;
}


- (void)doTransaction:(ACJSFunctionRef *)jsFunc completion:(void (^)(uexDatabaseTransactionResult))completion{
    dispatch_async(dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_DEFAULT, 0), ^{
        dispatch_semaphore_wait(self.transactionLock, DISPATCH_TIME_FOREVER);
        self.shouldRollback = NO;
        UEX_DO_IN_SERIAL_QUEUE_BEGIN;
        if (sqlite3_exec(_dbHandle, "BEGIN TRANSACTION", NULL, NULL, NULL) != SQLITE_OK){
            completion(uexDatabaseTransactionError);
            dispatch_semaphore_signal(self.transactionLock);
            return;
        }
        [jsFunc executeWithArguments:nil completionHandler:^(JSValue * _Nullable returnValue) {
            ACLogDebug(@"transaction infunc complete");
            dispatch_async(dispatch_get_main_queue(), ^{
                UEX_DO_IN_SERIAL_QUEUE_BEGIN;

                if (self.shouldRollback) {
                    sqlite3_exec(_dbHandle, "ROLLBACK TRANSACTION", NULL, NULL, NULL);
                    completion(uexDatabaseTransactionRollback);
                }else if(sqlite3_exec(_dbHandle, "COMMIT TRANSACTION", NULL, NULL, NULL) == SQLITE_OK){
                    completion(uexDatabaseTransactionSuccess);
                }else{
                    completion(uexDatabaseTransactionError);
                }
                dispatch_semaphore_signal(self.transactionLock);
                UEX_DO_IN_SERIAL_QUEUE_END;
            });
        }];
        UEX_DO_IN_SERIAL_QUEUE_END;
    });
}



@end
