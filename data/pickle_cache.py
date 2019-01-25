import os
import pickle


class CacheManager(object):
    def __init__(self, root_dir):
        # self._CHECK = False
        self.root_dir = root_dir
        if not os.path.exists(self.root_dir) or not os.path.isdir(self.root_dir):
            os.mkdir(self.root_dir)

        self._caches = {}

    def get(self, cache_name, init_load=True):
        if cache_name not in self._caches:
            path = os.path.join(self.root_dir, cache_name)
            cache = PickleCache(path)
            if init_load:
                cache.load()
            self._caches[cache_name] = cache
            print('cache manager get {}'.format(cache_name))
        # 如果在pcache被get之前就已经_enter过，那么_CHECK就失效了，with cache也不会load
        # 只要get触发就要重设_CHECK为False
        # self._CHECK = False
        return self._caches[cache_name]

    def load_all(self, force=False):
        for key, item in self._caches.items():
            # print('loading', key)
            item.load(force)

    def save_all(self, force=False):
        for key, item in self._caches.items():
            # print('saving', key)
            item.save(force)

    def clear_all(self):
        for key, item in self._caches.items():
            print('clearing', key)
            item.clear_cache()
            # item.delete_file()
            # print(key,'file deleted')

    def __enter__(self):
        print('流程开始.加载缓存')
        # if not self._CHECK:
        self.load_all()

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.save_all()
        if exc_traceback is not None:
            print('流程中断.')
            return False
        else:
            print('流程结束.')


class PickleCache(object):
    def __init__(self, cache_file_path):
        self.cache_dict = {}
        self.cache_file_path = cache_file_path
        self.changed = True

    def save(self, force=False):
        if self.changed or force:
            with open(self.cache_file_path, mode='wb') as fp:
                pickle.dump(self.cache_dict, fp)
                self.changed = False
                print('cache saved.{}'.format(self.cache_file_path))
        else:
            pass
            # print('no caches change.')

    def clear_cache(self):
        self.cache_dict = {}
        self.changed = True

    def delete_file(self):
        os.remove(self.cache_file_path)

    def load(self, force=False):
        if force:
            print(f'强制加载{self.cache_file_path}')
        elif self.changed is False:
            # print('cache already loaded.')
            return

        try:
            with open(self.cache_file_path, mode='rb') as fp:
                self.cache_dict = pickle.load(fp)
                self.changed = False
                print('cache loaded.{}'.format(self.cache_file_path))
        except FileNotFoundError:
            print('cache file not exists.save to get a new one.{}'.format(self.cache_file_path))
        except:
            self.clear_cache()
            print('cache loading error. clear and re-build.')

    def get(self, key):
        if not self.has(key):
            return None

        return self.cache_dict[key]

    def get_default_cache(self, key, default_func, *args, **kwargs):
        if not self.has(key):
            value = default_func(*args, **kwargs)
            self.set(key, value)
        return self.get(key)

    def set(self, key, data):
        self.cache_dict[key] = data
        self.changed = True

    def has(self, key):
        return key in self.cache_dict
