<?php

namespace app\admin\services\reseed;

use app\admin\services\client\ClientServices;
use app\enums\EventReseedEnums;
use app\model\Client;
use app\model\enums\DownloaderMarkerEnums;
use app\model\enums\ReseedStatusEnums;
use app\model\enums\ReseedSubtypeEnums;
use app\model\Reseed;
use app\model\Site;
use Error;
use Exception;
use Iyuu\BittorrentClient\ClientEnums;
use Iyuu\BittorrentClient\Clients;
use Iyuu\BittorrentClient\Contracts\Torrent as TorrentContract;
use Iyuu\SiteManager\Config;
use Iyuu\SiteManager\Contracts\Torrent;
use Iyuu\SiteManager\Spider\Helper;
use Rhilip\Bencode\Bencode;
use Rhilip\Bencode\ParseException;
use support\Log;
use Throwable;
use Webman\Event\Event;

/**
 * 自动辅种下载种子服务类
 */
class ReseedDownloadServices
{
    /**
     * 最大允许的sleep值，单位秒
     */
    private const int SLEEP_MAX_VALUE = 30;

    /**
     * 处理自动辅种表，下载种子塞给下载器
     * @param Site $site
     * @return void
     */
    public static function handle(Site $site): void
    {
        if (Reseed::getStatusEqDefault($site->sid)->doesntExist()) {
            // 无数据，返回
            return;
        }

        $config = new Config($site->toArray());
        $limit = $config->getLimit();
        if (empty($limit)) {
            // 不限速的站点
            self::handleOpen($site);
        } else {
            // 限速的站点
            $limitCount = $limit['count'] ?? 20;
            $limitSleep = $limit['sleep'] ?? 10;
            if (empty($limitCount)) {
                self::handleOpen($site, $limitSleep);
            } else {
                self::handleLimited($site, $limitCount, $limitSleep);
            }
        }
    }

    /**
     * 开放的站点
     * @param Site $site
     * @param int $limitSleep
     * @return void
     */
    private static function handleOpen(Site $site, int $limitSleep = 0): void
    {
        Reseed::getStatusEqDefault($site->sid)->chunkById(100, function ($records) use ($limitSleep) {
            /** @var Reseed $reseed */
            foreach ($records as $reseed) {
                // 更新：调度时间
                $reseed->dispatch_time = time();
                if (false === self::sendDownloader($reseed, $limitSleep)) {
                    return false;
                }
            }
            return true;
        });
    }

    /**
     * 限速的站点
     * @param Site $site 站点数据模型
     * @param int $limitCount 每天限制辅种总数量
     * @param int $limitSleep 每个种子间隔时间，单位:秒
     * @return void
     */
    private static function handleLimited(Site $site, int $limitCount, int $limitSleep): void
    {
        // 24小时内辅种数
        $total24h = Reseed::where('sid', '=', $site->id)
            ->where('dispatch_time', '>', time() - 86400)
            ->whereIn('status', [ReseedStatusEnums::Success->value, ReseedStatusEnums::Fail->value])
            ->count();

        if ($total24h < $limitCount) {
            Reseed::getStatusEqDefault($site->sid)->chunkById($limitCount - $total24h, function ($records) use ($limitSleep) {
                /** @var Reseed $reseed */
                foreach ($records as $reseed) {
                    // 更新：调度时间
                    $reseed->dispatch_time = time();
                    if (false === self::sendDownloader($reseed, $limitSleep)) {
                        return false;
                    }
                }
                return false;
            });
        }
    }

    /**
     * 发送到下载器
     * @param Reseed $reseed 自动辅种数据模型
     * @param int $limitSleep 每个种子间隔时间，单位:秒
     * @return bool
     */
    public static function sendDownloader(Reseed $reseed, int $limitSleep = 0): bool
    {
        $step = '';
        $cache = $reseed->reseedCache();
        try {
            // 如果间隔时间大于最大休眠 && 存在缓存，则等待下一个调度周期
            if (static::SLEEP_MAX_VALUE < $limitSleep && $cache->has()) {
                return false;
            }

            $torrent = new Torrent([
                'site' => $reseed->site,
                'id' => $reseed->reseed_id,
                'sid' => $reseed->sid,
                'torrent_id' => $reseed->torrent_id,
                'group_id' => $reseed->group_id,
            ]);
            $step = '1.准备下载种子';
            $response = Helper::download($torrent);
            $cache->set(time(), $limitSleep);
            $step = '2.种子下载成功';
            // 调度事件：下载种子之后
            Event::dispatch(EventReseedEnums::reseed_torrent_download_after->value, [$response, $reseed]);
            $step = '3.调度事件，下载种子后';

            $clientModel = ClientServices::getClient($reseed->client_id);
            $bittorrentClients = ClientServices::createBittorrent($clientModel);
            $contractsTorrent = new TorrentContract($response->payload, $response->metadata);
            $contractsTorrent->savePath = $reseed->directory;
            $reseedPayload = $reseed->getReseedPayload();

            // 调度事件：把种子发送给下载器之前
            $step = '4.调度事件，种子发送给下载器之前';
            self::validateBeforeSend($contractsTorrent, $bittorrentClients, $clientModel, $reseed, $reseedPayload);
            self::sendBefore($contractsTorrent, $bittorrentClients, $clientModel, $reseed);
            Event::dispatch(EventReseedEnums::reseed_torrent_send_before->value, [$contractsTorrent, $bittorrentClients, $clientModel, $reseed]);

            $result = $bittorrentClients->addTorrent($contractsTorrent);

            // 调度事件：把种子发送给下载器之后
            self::sendAfter($result, $bittorrentClients, $clientModel, $reseed);
            Event::dispatch(EventReseedEnums::reseed_torrent_send_after->value, [$result, $bittorrentClients, $clientModel, $reseed]);
            $step = '5.调度事件，种子发送给下载器之后';

            // 更新模型数据
            $reseed->message = is_string($result) ? $result : json_encode($result, JSON_UNESCAPED_UNICODE);
            $reseed->status = ReseedStatusEnums::Success->value;
            $reseed->save();

            return true;
        } catch (Error|Exception|Throwable $throwable) {
            $reseed->message = $step . ' ' . $throwable->getMessage();
            $reseed->status = ReseedStatusEnums::Fail->value;
            $reseed->save();
        } finally {
            // 如果大于0 && 小于等于最大休眠秒，直接sleep
            if (0 < $limitSleep && $limitSleep <= static::SLEEP_MAX_VALUE) {
                sleep(min($limitSleep, static::SLEEP_MAX_VALUE));
            }
        }

        return true;
    }

    /**
     * 把种子发送给下载器前，做一些操作
     * @param TorrentContract $contractsTorrent
     * @param Clients $bittorrentClients
     * @param Client $clientModel
     * @param Reseed $reseed
     * @return void
     */
    private static function sendBefore(TorrentContract $contractsTorrent, Clients $bittorrentClients, Client $clientModel, Reseed $reseed): void
    {
        $reseedPayload = $reseed->getReseedPayload();
        $markerEnum = $reseedPayload->getMarkerEnum();

        switch ($clientModel->getClientEnums()) {
            case ClientEnums::transmission:
                $contractsTorrent->parameters['paused'] = true;     // 添加任务校验后是否暂停
                if (DownloaderMarkerEnums::Empty !== $markerEnum) {
                    $contractsTorrent->parameters['labels'] = ['IYUU' . ReseedSubtypeEnums::text($reseed->getSubtypeEnums())];   // 添加分类标签
                }
                break;
            case ClientEnums::qBittorrent;
                $contractsTorrent->parameters['autoTMM'] = 'false'; // 关闭自动种子管理
                $contractsTorrent->parameters['paused'] = 'true';   // 添加任务校验后是否暂停
                if ($reseedPayload->isQbSkipCheck()) {
                    $contractsTorrent->parameters['skip_checking'] = 'true';
                }
                if (DownloaderMarkerEnums::Category === $markerEnum) {
                    $contractsTorrent->parameters['category'] = 'IYUU' . ReseedSubtypeEnums::text($reseed->getSubtypeEnums());   // 添加分类标签
                }
                $contractsTorrent->parameters['root_folder'] = $clientModel->root_folder ? 'true' : 'false';    // 是否创建根目录
                break;
        }
    }

    /**
     * 把种子发送给下载器之后，做一些操作
     * @param mixed $result
     * @param Clients $bittorrentClients
     * @param Client $clientModel
     * @param Reseed $reseed
     * @return void
     */
    private static function sendAfter(mixed $result, Clients $bittorrentClients, Client $clientModel, Reseed $reseed): void
    {
        try {
            $reseedPayload = $reseed->getReseedPayload();
            $markerEnum = $reseedPayload->getMarkerEnum();
            switch ($clientModel->getClientEnums()) {
                case ClientEnums::qBittorrent:
                    if (is_string($result) && str_contains(strtolower($result), 'ok')) {
                        $retry = 5;
                        do {
                            try {
                                sleep(5);
                                /** @var \Iyuu\BittorrentClient\Driver\qBittorrent\Client $bittorrentClients */
                                // 标记标签 2024年4月25日
                                if (DownloaderMarkerEnums::Tag === $markerEnum) {
                                    $bittorrentClients->torrentAddTags($reseed->info_hash, 'IYUU' . ReseedSubtypeEnums::text($reseed->getSubtypeEnums()));
                                }

                                // 发送校验命令
                                if ($reseedPayload->isAutoCheck() && !$reseedPayload->isQbSkipCheck()) {
                                    $bittorrentClients->recheck($reseed->info_hash);
                                }
                                $retry = 0;
                            } catch (Throwable $throwable) {
                                Log::debug('自动辅种 标记标签和校验指令 发送失败，正在重试 | 递减值' . $retry . ' | ' . $throwable->getMessage());
                            }
                        } while (0 < $retry--);
                    }
                    break;
                default:
                    break;
            }
        } catch (Throwable $throwable) {
            Log::error('把种子发送给下载器之后，做一些操作，异常啦：' . $throwable->getMessage());
        }
    }

    /**
     * 下发前校验
     * @param TorrentContract $contractsTorrent
     * @param Clients $bittorrentClients
     * @param Client $clientModel
     * @param Reseed $reseed
     * @param \app\model\payload\ReseedPayload $reseedPayload
     * @return void
     */
    private static function validateBeforeSend(TorrentContract $contractsTorrent, Clients $bittorrentClients, Client $clientModel, Reseed $reseed, \app\model\payload\ReseedPayload $reseedPayload): void
    {
        if ($clientModel->getClientEnums() !== ClientEnums::qBittorrent || !$reseedPayload->isQbSkipCheck()) {
            return;
        }

        if (!$bittorrentClients instanceof \Iyuu\BittorrentClient\Driver\qBittorrent\Client) {
            throw new InvalidArgumentException('qBittorrent下载器实例异常，无法执行跳校验严格比对');
        }

        $sourceInfoHash = trim($reseedPayload->source_info_hash);
        if ($sourceInfoHash === '') {
            throw new InvalidArgumentException('缺少源种子infohash，已拦截qB跳校验任务');
        }

        [$targetTitle, $targetSize, $targetFiles] = self::parseTorrentMetadata($contractsTorrent->payload);
        [$sourceTitle, $sourceSize, $sourceFiles] = self::fetchQbSourceMeta($bittorrentClients, $sourceInfoHash);

        if ($sourceTitle !== $targetTitle) {
            throw new InvalidArgumentException("qB跳校验已拦截：标题不一致（源：{$sourceTitle}，目标：{$targetTitle}）");
        }
        if ($sourceSize !== $targetSize) {
            throw new InvalidArgumentException("qB跳校验已拦截：大小不一致（源：{$sourceSize}，目标：{$targetSize}）");
        }
        if ($sourceFiles !== $targetFiles) {
            throw new InvalidArgumentException('qB跳校验已拦截：文件列表不一致');
        }

        Log::info("qB跳校验严格比对通过：{$reseed->info_hash} <= {$sourceInfoHash}");
    }

    /**
     * 解析种子元数据
     * @param string $payload
     * @return array{0:string,1:int,2:array}
     */
    private static function parseTorrentMetadata(string $payload): array
    {
        try {
            $torrent = Bencode::decode($payload);
        } catch (ParseException $exception) {
            throw new InvalidArgumentException('目标种子元数据解析失败：' . $exception->getMessage());
        }

        if (empty($torrent['info']) || !is_array($torrent['info'])) {
            throw new InvalidArgumentException('目标种子元数据缺少info字段');
        }
        $info = $torrent['info'];
        $title = (string)($info['name'] ?? '');
        if ($title === '') {
            throw new InvalidArgumentException('目标种子元数据缺少标题');
        }

        $files = [];
        $size = 0;
        if (!empty($info['files']) && is_array($info['files'])) {
            foreach ($info['files'] as $file) {
                if (!is_array($file)) {
                    continue;
                }
                $length = (int)($file['length'] ?? 0);
                $parts = $file['path'] ?? [];
                $path = is_array($parts) ? implode('/', $parts) : (string)$parts;
                $path = self::normalizePath($path);
                $files[] = $path . '|' . $length;
                $size += $length;
            }
        } else {
            $length = (int)($info['length'] ?? 0);
            $files[] = self::normalizePath($title) . '|' . $length;
            $size = $length;
        }
        sort($files);
        return [$title, $size, $files];
    }

    /**
     * 获取qB源种子元数据
     * @param \Iyuu\BittorrentClient\Driver\qBittorrent\Client $qb
     * @param string $hash
     * @return array{0:string,1:int,2:array}
     */
    private static function fetchQbSourceMeta(\Iyuu\BittorrentClient\Driver\qBittorrent\Client $qb, string $hash): array
    {
        $list = json_decode((string)$qb->getData('torrent_list', ['hashes' => $hash]), true);
        if (empty($list) || !is_array($list) || empty($list[0])) {
            throw new InvalidArgumentException("qB跳校验已拦截：源种子不存在 {$hash}");
        }
        $item = $list[0];
        $title = (string)($item['name'] ?? '');
        $size = (int)($item['size'] ?? $item['total_size'] ?? 0);
        if ($title === '' || $size <= 0) {
            throw new InvalidArgumentException('qB跳校验已拦截：源种子标题或大小读取失败');
        }

        $filesRaw = json_decode((string)$qb->getData('torrent_files', ['hash' => $hash]), true);
        if (!is_array($filesRaw)) {
            throw new InvalidArgumentException('qB跳校验已拦截：源种子文件列表读取失败');
        }

        $files = [];
        foreach ($filesRaw as $file) {
            if (!is_array($file)) {
                continue;
            }
            $path = (string)($file['name'] ?? $file['path'] ?? '');
            $path = self::normalizePath($path);
            $titlePrefix = self::normalizePath($title) . '/';
            if (str_starts_with($path, $titlePrefix)) {
                $path = substr($path, strlen($titlePrefix));
            }
            $files[] = $path . '|' . (int)($file['size'] ?? 0);
        }
        sort($files);

        return [$title, $size, $files];
    }

    /**
     * 规范化路径
     * @param string $path
     * @return string
     */
    private static function normalizePath(string $path): string
    {
        return trim(str_replace('\\', '/', $path), '/');
    }
}
